# -*- coding: utf-8 -*-
"""
Worker que consome validations_jobs com validação SBCP.
Regras novas:
- Lê do membersnextlevel (metadata) um "documento" que pode ser RQE/CRM/CREFITO.
- Se o documento estiver vazio -> NÃO consulta -> pendencia direta.
- Se houver documento -> consulta -> aprova apenas se algum ID retornado (rqe/crm/crefito)
  casar com o documento informado (considerando UF opcional tipo 98675-MG).
"""

import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp, log_validation, set_member_validation  # mantém o fluxo atual
# buscar_sbcp já busca pelo nome; set_member_validation grava validacao_acesso/portal_validado. :contentReference[oaicite:2]{index=2} :contentReference[oaicite:3]{index=3}

DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

# -----------------------
# DB helpers
# -----------------------
def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def fetch_next_job(conn) -> Optional[Dict[str, Any]]:
    """Seleciona 1 job PENDING com SKIP LOCKED para evitar concorrência."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, member_id, email, nome, fonte, status, attempts
              FROM validations_jobs
             WHERE status = 'PENDING'
             ORDER BY id
             FOR UPDATE SKIP LOCKED
             LIMIT 1
            """
        )
        row = cur.fetchone()
        return dict(row) if row else None

def mark_running(conn, job_id: int, attempts: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validations_jobs
               SET status = 'RUNNING',
                   attempts = %s
             WHERE id = %s
            """,
            (attempts + 1, job_id),
        )

def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validations_jobs
               SET status = %s,
                   last_error = %s
             WHERE id = %s
            """,
            (status, last_error, job_id),
        )

# -----------------------
# Normalização & matching
# -----------------------
UF_RE = r"(?:-[A-Z]{2}|/[A-Z]{2})$"  # sufixo tipo -MG /MG

def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def split_number_uf(s: Optional[str]) -> Tuple[str, Optional[str]]:
    """
    Retorna (numero_digits, UF_optional).
    Aceita '98675-MG', '98675/MG' ou '98675' → ('98675', 'MG') ou ('98675', None)
    """
    s = (s or "").strip().upper()
    if not s:
        return "", None
    m = re.search(r"^(.+?)(?:-|/)([A-Z]{2})$", s)
    if m:
        numero = only_digits(m.group(1))
        uf = m.group(2)
        return numero, uf
    return only_digits(s), None

def pick_member_document(conn, member_id: int) -> str:
    """
    Busca um possível documento do membro nas colunas/metadata.
    Considera metadados 'rqe', 'crm', 'crefito', 'doc' (se existir).
    Retorna string (pode estar com UF).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(metadata->>'rqe','')    AS rqe,
                COALESCE(metadata->>'crm','')    AS crm,
                COALESCE(metadata->>'crefito','')AS crefito,
                COALESCE(metadata->>'doc','')    AS doc
            FROM membersnextlevel
            WHERE id = %s
            """,
            (member_id,),
        )
        row = cur.fetchone()
        if not row:
            return ""
        # ordem de preferência: doc -> rqe -> crm -> crefito
        doc = row[3] or row[0] or row[1] or row[2] or ""
        return doc.strip()

def collect_identifiers_from_result(result: Dict[str, Any]) -> Set[str]:
    """
    Extrai possíveis IDs (rqe/crm/crefito) do retorno do scraper.
    Cria um conjunto com variações equivalentes:
      - apenas dígitos
      - dígitos+UF quando houver
    """
    ids: Set[str] = set()

    resultados = result.get("resultados") or []
    if not isinstance(resultados, list) or not resultados:
        return ids

    # olhamos somente o primeiro perfil (como o fluxo atual já faz)
    d = resultados[0] or {}

    # candidatos de chaves (case-insensitive)
    keys = [k for k in d.keys() if isinstance(k, str)]

    def add_variants(v: Optional[str]):
        if not v:
            return
        num, uf = split_number_uf(str(v))
        if not num:
            return
        ids.add(num)                  # sem UF
        if uf:
            ids.add(f"{num}-{uf}")    # com UF padronizado com '-'

    # heurística: qualquer chave que contenha rqe/crm/crefito
    for k in keys:
        lk = k.lower()
        if "rqe" in lk or "crm" in lk or "crefito" in lk:
            add_variants(str(d.get(k)))

    # fallback: alguns scrapers consolidam *_padrao
    for k in ("rqe_padrao", "crm_padrao", "crefito_padrao"):
        if k in d:
            add_variants(str(d.get(k)))

    return ids

def match_document(expected: str, extracted_ids: Set[str]) -> bool:
    """
    Regra de equivalência:
      - expected '98675' casa com '98675' e com '98675-MG'
      - expected '98675-MG' casa com '98675' e '98675-MG'
    """
    if not expected:
        return False

    num, uf = split_number_uf(expected)

    # candidato(s) equivalentes
    candidates = {num}
    if uf:
        candidates.add(f"{num}-{uf.upper()}")

    # match se qualquer candidato estiver no conjunto extraído
    # ou se o conjunto extraído contiver apenas o número (sem UF)
    for cand in candidates:
        if cand in extracted_ids:
            return True

    # tolerância extra: se extraído tiver apenas dígitos e expected tiver UF, ou vice-versa
    if num in extracted_ids:
        return True

    return False

# -----------------------
# Main loop
# -----------------------
def work_loop():
    print(f"🧵 Worker de validação iniciado.\nConfig: POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS}")
    conn = db()

    while True:
        try:
            # Tenta pegar 1 job
            with conn:
                job = fetch_next_job(conn)

                if not job:
                    time.sleep(POLL_SECONDS)
                    continue

                job_id = job["id"]
                member_id = job["member_id"]
                email = job.get("email") or ""
                nome = job.get("nome") or ""
                fonte = job.get("fonte") or "sbcp"   # mantém default do pipeline atual :contentReference[oaicite:4]{index=4}
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    continue

                print(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id} fonte={fonte}]")
                mark_running(conn, job_id, attempts)

            # Executa fora do bloco de transação longa
            steps: List[str] = []
            result: Dict[str, Any] = {}
            last_error: Optional[str] = None

            # 1) Lê documento esperado do BD
            try:
                expected_doc = pick_member_document(conn, member_id).strip()
            except Exception as e_doc:
                expected_doc = ""
                steps.append(f"expected_doc_read_error:{e_doc}")

            if not expected_doc:
                # Sem documento no BD -> regra: pendencia direta (sem consultar)
                result = {"ok": False, "steps": steps + ["sem_documento_no_bd"], "resultados": [], "qtd": 0}
                status_log = "error"
            else:
                # 2) Chama a fonte solicitada
                try:
                    if fonte == "sbcp":
                        result = buscar_sbcp(member_id, nome, email, steps)  # usa apenas o NOME internamente :contentReference[oaicite:5]{index=5}
                    else:
                        steps.append(f"fonte_desconhecida:{fonte}")
                        result = {"ok": False, "steps": steps, "resultados": [], "qtd": 0}
                except Exception as e:
                    last_error = f"exception:{e}"
                    result = {"ok": False, "steps": steps + [last_error], "resultados": [], "qtd": 0}

                # 3) Compara IDs: aprova só se houver match
                extracted_ids = collect_identifiers_from_result(result)
                if extracted_ids:
                    steps.append(f"ids_extraidos={sorted(list(extracted_ids))[:6]}")
                else:
                    steps.append("ids_extraidos=vazio")

                is_match = match_document(expected_doc, extracted_ids)
                if is_match:
                    result["ok"] = True
                    status_log = "ok"
                    steps.append(f"match_ok expected='{expected_doc}'")
                else:
                    result["ok"] = False
                    status_log = "error"
                    steps.append(f"match_fail expected='{expected_doc}'")

            # 4) Log + atualização de status do membro
            try:
                log_validation(conn, member_id, fonte, status_log, {"raw": result, "fonte": fonte})
                if result.get("ok"):
                    set_member_validation(conn, member_id, "aprovado", fonte)
                else:
                    set_member_validation(conn, member_id, "pendente", fonte)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # 5) Finaliza o job
            with conn:
                if result.get("ok"):
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    print(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado/{fonte})")
                else:
                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        print(f"🔁 Job {job_id} re-enfileirado (retry).")
                    else:
                        finalize_job(conn, job_id, "FAILED", last_error or "erro_definitivo")
                        print(f"🧯 Job {job_id} -> FAILED definitivo.")

        except Exception as outer:
            # Erro geral do loop
            print(f"💥 Loop erro: {outer}")
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    work_loop()
