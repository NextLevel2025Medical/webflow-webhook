# -*- coding: utf-8 -*-
"""
Worker de validação que consome validations_jobs e valida no site da SBCP.
Regras:
- Lê do membersnextlevel.metadata um "documento" (rqe/crm/crefito ou doc).
- Se o documento estiver vazio -> NÃO consulta -> pendente direta.
- Se houver documento -> consulta -> aprova somente se algum ID retornado
  (rqe/crm/crefito, com equivalência com/sem UF) casar com o documento informado.
- Grava logs mais conclusivos em validations_log.
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp, log_validation, set_member_validation

DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

# =========================
# Conexão / helpers de BD
# =========================
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
               SET status = 'RUNNING', attempts = %s
             WHERE id = %s
            """,
            (attempts + 1, job_id),
        )

def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validations_jobs
               SET status = %s, last_error = %s
             WHERE id = %s
            """,
            (status, last_error, job_id),
        )

# =========================
# Normalização & matching
# =========================
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
                COALESCE(metadata->>'rqe','')     AS rqe,
                COALESCE(metadata->>'crm','')     AS crm,
                COALESCE(metadata->>'crefito','') AS crefito,
                COALESCE(metadata->>'doc','')     AS doc
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
      - dígitos+UF quando houver (padroniza com '-')
    """
    ids: Set[str] = set()
    resultados = result.get("resultados") or []
    if not isinstance(resultados, list) or not resultados:
        return ids

    d = resultados[0] or {}
    keys = [k for k in d.keys() if isinstance(k, str)]

    def add_variants(v: Optional[str]):
        if not v:
            return
        num, uf = split_number_uf(str(v))
        if not num:
            return
        ids.add(num)               # sem UF
        if uf:
            ids.add(f"{num}-{uf}") # com UF padronizado

    # heurística: qualquer chave que contenha rqe/crm/crefito
    for k in keys:
        lk = k.lower()
        if "rqe" in lk or "crm" in lk or "crefito" in lk:
            add_variants(str(d.get(k)))

    # campos *_padrao (se existirem)
    for k in ("rqe_padrao", "crm_padrao", "crefito_padrao"):
        if k in d:
            add_variants(str(d.get(k)))

    return ids

def match_document(expected: str, extracted_ids: Set[str]) -> bool:
    """
    Regras de equivalência:
      - expected '98675' casa com '98675' e com '98675-MG'
      - expected '98675-MG' casa com '98675' e '98675-MG'
    """
    if not expected:
        return False
    num, uf = split_number_uf(expected)
    candidates = {num}
    if uf:
        candidates.add(f"{num}-{uf.upper()}")

    for cand in candidates:
        if cand in extracted_ids:
            return True
    if num in extracted_ids:
        return True
    return False

# =========================
# Loop principal
# =========================
def work_loop():
    print(f"🧵 Worker de validação iniciado. POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS}")
    conn = db()

    while True:
        try:
            with conn:
                job = fetch_next_job(conn)
                if not job:
                    time.sleep(POLL_SECONDS)
                    continue

                job_id = job["id"]
                member_id = job["member_id"]
                email = job.get("email") or ""
                nome = job.get("nome") or ""
                # Default da fonte; ajuste para "cirurgiaplastica.org.br" se quiser etiquetar assim
                fonte = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    continue

                print(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id} fonte={fonte}]")
                mark_running(conn, job_id, attempts)

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
                # Sem documento no BD -> pendente direto
                result = {
                    "ok": False,
                    "steps": steps + ["sem_documento_no_bd"],
                    "resultados": [],
                    "qtd": 0,
                    "expected_doc": "",
                    "reason": "pendente_sem_documento",
                }
                status_log = "pendente_sem_documento"

            else:
                # 2) Chama a fonte
                try:
                    if fonte == "sbcp":
                        result = buscar_sbcp(member_id, nome, email, steps)
                    else:
                        steps.append(f"fonte_desconhecida:{fonte}")
                        result = {"ok": False, "steps": steps, "resultados": [], "qtd": 0, "reason": "fonte_desconhecida"}

                    # 3) Coleta IDs extraídos e faz matching
                    extracted_ids = collect_identifiers_from_result(result)
                    if extracted_ids:
                        steps.append(f"ids_extraidos={sorted(list(extracted_ids))[:8]}")
                    else:
                        steps.append("ids_extraidos=vazio")

                    is_match = match_document(expected_doc, extracted_ids)
                    result["expected_doc"] = expected_doc
                    result["match"] = bool(is_match)

                    if result.get("reason") == "sem_resultados_ou_layout_alterado":
                        status_log = "sem_resultados"
                        result["ok"] = False
                    elif is_match:
                        status_log = "ok"
                        result["ok"] = True
                    else:
                        status_log = "numero_registro_invalido"
                        result["ok"] = False

                except Exception as e:
                    last_error = f"exception:{e}"
                    result = {"ok": False, "steps": steps + [last_error], "resultados": [], "qtd": 0}
                    status_log = "error_execucao"

            # 4) Log + atualização do membro
            try:
                log_validation(conn, member_id, fonte, status_log, {"raw": result, "fonte": fonte})
                if result.get("ok"):
                    set_member_validation(conn, member_id, "aprovado", fonte)
                else:
                    set_member_validation(conn, member_id, "pendente", fonte)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # 5) Finaliza job
            with conn:
                if result.get("ok"):
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    print(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado/{fonte})")
                else:
                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        print(f"🔁 Job {job_id} re-enfileirado (retry). status_log={status_log}")
                    else:
                        finalize_job(conn, job_id, "FAILED", last_error or status_log or "erro_definitivo")
                        print(f"🧯 Job {job_id} -> FAILED definitivo. status_log={status_log}")

        except Exception as outer:
            print(f"💥 Loop erro: {outer}")
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    work_loop()
