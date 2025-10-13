# -*- coding: utf-8 -*-
"""
Worker de validação que consome validations_jobs e valida no site da SBCP.

Fluxo:
1) Lê um job PENDING (SKIP LOCKED).
2) Pega o documento do membro (ordem de preferência: doc -> rqe -> crm -> crefito).
3) Se não houver documento: marca como pendente e re-enfileira até MAX_ATTEMPTS.
4) Se houver documento: chama buscar_sbcp(...), extrai IDs do perfil e compara.
5) Loga em validations_log e atualiza membersnextlevel.status_validation.
6) SUCCEEDED se casou; caso contrário re-enfileira até MAX_ATTEMPTS e então FAILED.

Compatível com a versão nova do consulta_medicos.py:
- Usa result["dados"] (novo) e tem fallback para result["resultados"] (antigo).
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
    Divide "12345-MG", "12345/MG", "12345 MG" em ('12345','MG').
    Se não houver UF, retorna ('12345', None).
    """
    s = (s or "").strip().upper()
    if not s:
        return "", None
    m = re.search(r"^(.+?)(?:-|/|\s)([A-Z]{2})$", s)
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
    Extrai possíveis IDs (rqe/crm/crefito) do retorno do scraper e gera variantes equivalentes:
      - apenas dígitos (ex.: "32019")
      - dígitos+UF com hífen quando houver (ex.: "32019-BA")
    Compatível com:
      - NOVO: result["dados"] (campos: crm/rqe/crefito, *_padrao e listas *_padrao)
      - ANTIGO: result["resultados"][0] (mesma ideia)
    """
    ids: Set[str] = set()

    def add_value(v: Optional[str]):
        if not v:
            return
        num, uf = split_number_uf(str(v))
        if not num:
            return
        ids.add(num)  # só dígitos
        if uf:
            ids.add(f"{num}-{uf}")

    def add_list(lst: Optional[List[str]]):
        if not lst:
            return
        for v in lst:
            add_value(v)

    # --- NOVO formato: dicionário em result["dados"] ---
    d = result.get("dados") or {}
    if isinstance(d, dict) and d:
        # preferir *_padrao quando houver
        add_value(d.get("crm_padrao") or d.get("crm"))
        add_value(d.get("rqe_padrao") or d.get("rqe"))
        add_value(d.get("crefito_padrao") or d.get("crefito"))

        add_list(d.get("crms_padrao") or d.get("crms"))
        add_list(d.get("rqes_padrao") or d.get("rqes"))
        add_list(d.get("crefitos_padrao") or d.get("crefitos"))

    # --- Fallback: antigo result["resultados"][0] ---
    if not ids:
        resultados = result.get("resultados") or []
        if isinstance(resultados, list) and resultados:
            d0 = resultados[0] or {}
            if isinstance(d0, dict):
                add_value(d0.get("crm_padrao") or d0.get("crm"))
                add_value(d0.get("rqe_padrao") or d0.get("rqe"))
                add_value(d0.get("crefito_padrao") or d0.get("crefito"))
                add_list(d0.get("crms_padrao") or d0.get("crms"))
                add_list(d0.get("rqes_padrao") or d0.get("rqes"))
                add_list(d0.get("crefitos_padrao") or d0.get("crefitos"))

    return ids

def match_document(expected: str, extracted_ids: Set[str]) -> bool:
    """
    Verifica se o documento esperado aparece no conjunto extraído,
    considerando equivalência com/sem UF.
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

    # casa com qualquer variante presente
    for cand in candidates:
        if cand in extracted_ids:
            return True
    # fallback: se só o número estiver presente
    if num in extracted_ids:
        return True
    return False

# =========================
# Loop principal
# =========================
def work_loop():
    conn = db()
    print("🚀 worker_validation iniciado")

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
                # Default da fonte; pode ajustar p/ "cirurgiaplastica.org.br" se preferir
                fonte = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    print(f"🧯 Job {job_id} -> FAILED (tentativas_excedidas)")
                    continue

                print(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id} fonte={fonte}]")
                mark_running(conn, job_id, attempts)

            steps: List[str] = []
            result: Dict[str, Any] = {}
            last_error: Optional[str] = None
            status_log: str = "init"

            # 1) Lê documento esperado do BD
            try:
                expected_doc = pick_member_document(conn, member_id).strip()
                if not expected_doc:
                    status_log = "sem_documento"
                    result = {
                        "ok": False,
                        "reason": "documento_vazio",
                        "steps": steps + ["documento_vazio"],
                    }
                else:
                    steps.append(f"expected_doc={expected_doc}")
            except Exception as e:
                last_error = f"db_erro:{e}"
                status_log = "db_erro"
                result = {"ok": False, "reason": "db_erro", "steps": steps}

            # 2) Se houver documento, roda a consulta
            if result.get("reason") != "documento_vazio" and not last_error:
                try:
                    result = buscar_sbcp(member_id=member_id, nome=nome, email=email, steps=steps)
                    status_log = "executado"
                except Exception as e:
                    last_error = f"exec_erro:{e}"
                    status_log = "error_execucao"
                    result = {"ok": False, "reason": "error_execucao", "steps": steps}

            # 3) Decisão (match)
            if not last_error and result and result.get("reason") != "documento_vazio":
                try:
                    extracted_ids = collect_identifiers_from_result(result)
                    if extracted_ids:
                        steps.append(f"ids_extraidos={sorted(list(extracted_ids))}")
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
                    last_error = f"match_erro:{e}"
                    status_log = "match_erro"
                    result["ok"] = False

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
