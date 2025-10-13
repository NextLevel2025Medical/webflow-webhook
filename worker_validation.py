# -*- coding: utf-8 -*-
"""
Worker de validação:
- Consome validations_jobs
- Valida no site (consulta_medicos.buscar_sbcp)
- Atualiza membersnextlevel.status_validation
- Ao final do job:
    * APROVADO  -> envia BotConversa flow 7479824
    * PENDENTE/RECUSADO (FAILED final) -> envia BotConversa flow 7479965
  O envio usa o subscriber_id salvo no metadata (botconversa_id). Se não houver, cria/atualiza subscriber.

Variáveis de ambiente (com defaults):
- DATABASE_URL
- BOTCONVERSA_API_KEY (default: chave fornecida)
- BOTCONVERSA_BASE_URL (default: https://backend.botconversa.com.br)
- BOTCONVERSA_FLOW_APROVADO (default: 7479824)
- BOTCONVERSA_FLOW_PENDENTE  (default: 7479965)
- POLL_SECONDS (default: 3)
- MAX_ATTEMPTS (default: 3)
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp

# ------------------ Config ------------------
DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

BOTCONVERSA_API_KEY = os.getenv(
    "BOTCONVERSA_API_KEY",
    "362e173a-ba27-4655-9191-b4fd735394da"  # ideal: mover para env
)
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
FLOW_APROVADO = int(os.getenv("BOTCONVERSA_FLOW_APROVADO", "7479824"))
FLOW_PENDENTE = int(os.getenv("BOTCONVERSA_FLOW_PENDENTE", "7479965"))

# ------------------ BD ------------------
def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def fetch_next_job(conn) -> Optional[Dict[str, Any]]:
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
            "UPDATE validations_jobs SET status='RUNNING', attempts=%s, updated_at=NOW() WHERE id=%s",
            (attempts + 1, job_id),
        )

def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE validations_jobs SET status=%s, last_error=%s, updated_at=NOW() WHERE id=%s",
            (status, last_error, job_id),
        )

# ------------------ Member helpers ------------------
def get_member_core(conn, member_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, email, nome, metadata FROM membersnextlevel WHERE id=%s",
            (member_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None

def set_member_validation(conn, member_id: int, status: str, fonte: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET status_validation = %s,
                   fonte_validation  = %s,
                   updated_at        = NOW()
             WHERE id = %s
            """,
            (status, fonte, member_id),
        )

def save_member_botconversa_id(conn, member_id: int, subscriber_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb,
                   updated_at = NOW()
             WHERE id = %s
            """,
            (psycopg2.extras.Json({"botconversa_id": subscriber_id}), member_id),
        )

# ------------------ Normalização / matching ------------------
def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def split_number_uf(s: Optional[str]) -> Tuple[str, Optional[str]]:
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
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(metadata->>'doc','')     AS doc,
                COALESCE(metadata->>'rqe','')     AS rqe,
                COALESCE(metadata->>'crm','')     AS crm,
                COALESCE(metadata->>'crefito','') AS crefito
            FROM membersnextlevel
            WHERE id = %s
            """,
            (member_id,),
        )
        row = cur.fetchone()
        if not row:
            return ""
        doc = (row[0] or row[1] or row[2] or row[3] or "").strip()
        return doc

def collect_identifiers_from_result(result: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()

    def add_value(v: Optional[str]):
        if not v:
            return
        num, uf = split_number_uf(str(v))
        if not num:
            return
        ids.add(num)
        if uf:
            ids.add(f"{num}-{uf}")

    def add_list(lst: Optional[List[str]]):
        if not lst:
            return
        for v in lst:
            add_value(v)

    d = result.get("dados") or {}
    if isinstance(d, dict) and d:
        add_value(d.get("crm_padrao") or d.get("crm"))
        add_value(d.get("rqe_padrao") or d.get("rqe"))
        add_value(d.get("crefito_padrao") or d.get("crefito"))
        add_list(d.get("crms_padrao") or d.get("crms"))
        add_list(d.get("rqes_padrao") or d.get("rqes"))
        add_list(d.get("crefitos_padrao") or d.get("crefitos"))

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

# ------------------ BotConversa ------------------
def bc_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Content-Type": "application/json",
        "API-KEY": BOTCONVERSA_API_KEY,
    }

def bc_create_or_update_subscriber(phone: str, first_name: str, last_name: str) -> Optional[int]:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    payload = {"phone": phone, "first_name": first_name, "last_name": last_name}
    try:
        r = requests.post(url, headers=bc_headers(), json=payload, timeout=20)
        if not r.ok:
            print("BotConversa subscriber FAIL", r.status_code, r.text, flush=True)
            return None
        data = r.json()
        sid = data.get("id")
        try:
            return int(sid)
        except Exception:
            return None
    except Exception as e:
        print("BotConversa subscriber EXC", repr(e), flush=True)
        return None

def bc_send_flow(subscriber_id: int, flow_id: int) -> bool:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/{subscriber_id}/send_flow/"
    try:
        r = requests.post(url, headers=bc_headers(), json={"flow": flow_id}, timeout=20)
        if not r.ok:
            print("BotConversa send_flow FAIL", r.status_code, r.text, flush=True)
        return bool(r.ok)
    except Exception as e:
        print("BotConversa send_flow EXC", repr(e), flush=True)
        return False

def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def ensure_subscriber_id(conn, member: Dict[str, Any]) -> Optional[int]:
    """
    Garante que teremos um subscriber_id:
    - se já houver em metadata -> usa
    - senão, tenta criar usando phone e nome
    - salva no metadata quando criar
    """
    metadata = member.get("metadata") or {}
    sid = metadata.get("botconversa_id")
    if sid:
        try:
            return int(sid)
        except Exception:
            pass

    phone = (metadata.get("phone") or "").strip()
    first_name, last_name = split_name(member.get("nome") or "")
    if not phone:
        return None

    sid = bc_create_or_update_subscriber(phone=only_digits(phone), first_name=first_name, last_name=last_name)
    if sid:
        save_member_botconversa_id(conn, member["id"], sid)
    return sid

# ------------------ Reaper opcional ------------------
def reset_stale_running(conn, minutes: int = 10) -> int:
    total = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE validations_jobs
                   SET status='PENDING', last_error='stale_running_reset', updated_at=NOW()
                 WHERE status='RUNNING'
                   AND COALESCE(updated_at, created_at, NOW() - INTERVAL '1 day')
                       < NOW() - (%s || ' minutes')::interval
                """,
                (minutes,),
            )
            total = cur.rowcount or 0
    except Exception:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE validations_jobs SET status='PENDING' WHERE status='RUNNING'"
                )
                total = cur.rowcount or 0
        except Exception:
            total = 0
    return total

# ------------------ Loop principal ------------------
def work_loop():
    conn = db()
    print("🚀 worker_validation iniciado", flush=True)

    freed = reset_stale_running(conn, minutes=10)
    if freed:
        print(f"🧹 Jobs RUNNING destravados na inicialização: {freed}", flush=True)

    while True:
        try:
            # Seleciona job
            with conn:
                job = fetch_next_job(conn)
                if not job:
                    time.sleep(POLL_SECONDS)
                    continue

                job_id = job["id"]
                member_id = job["member_id"]
                email = job.get("email") or ""
                nome = job.get("nome") or ""
                fonte = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    print(f"🧯 Job {job_id} -> FAILED (tentativas_excedidas)", flush=True)
                    continue

                print(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id}]", flush=True)
                mark_running(conn, job_id, attempts)

            steps: List[str] = []
            result: Dict[str, Any] = {}
            last_error: Optional[str] = None
            status_log: str = "init"

            # Documento esperado
            try:
                expected_doc = pick_member_document(conn, member_id).strip()
                if not expected_doc:
                    status_log = "sem_documento"
                    result = {"ok": False, "reason": "documento_vazio", "steps": steps + ["documento_vazio"]}
                else:
                    steps.append(f"expected_doc={expected_doc}")
            except Exception as e:
                last_error = f"db_erro:{e}"
                status_log = "db_erro"
                result = {"ok": False, "reason": "db_erro", "steps": steps}

            # Busca SBCP
            if result.get("reason") != "documento_vazio" and not last_error:
                try:
                    result = buscar_sbcp(member_id=member_id, nome=nome, email=email, steps=steps)
                    status_log = "executado"
                except Exception as e:
                    last_error = f"exec_erro:{e}"
                    status_log = "error_execucao"
                    result = {"ok": False, "reason": "error_execucao", "steps": steps}

            # Match
            if not last_error and result and result.get("reason") != "documento_vazio":
                try:
                    extracted_ids = collect_identifiers_from_result(result)
                    steps.append(f"ids_extraidos={sorted(list(extracted_ids))}" if extracted_ids else "ids_extraidos=vazio")

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

            # Persistência (status do membro)
            try:
                if result.get("ok"):
                    set_member_validation(conn, member_id, "aprovado", fonte)
                else:
                    set_member_validation(conn, member_id, "pendente", fonte)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # Finalização do job + envio do flow FINAL
            with conn:
                member = get_member_core(conn, member_id) or {"id": member_id, "nome": nome, "metadata": {}}

                if result.get("ok"):
                    # SUCCEEDED -> envia flow aprovado
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    print(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado)", flush=True)

                    sid = ensure_subscriber_id(conn, member)
                    if sid:
                        bc_send_flow(sid, FLOW_APROVADO)
                    else:
                        print("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow aprovado.", flush=True)

                else:
                    # Falhou nesta tentativa
                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        print(f"🔁 Job {job_id} re-enfileirado (retry). status_log={status_log}", flush=True)
                    else:
                        # FAILED definitivo -> envia flow pendente/recusado
                        finalize_job(conn, job_id, "FAILED", last_error or status_log or "erro_definitivo")
                        print(f"🧯 Job {job_id} -> FAILED definitivo. status_log={status_log}", flush=True)

                        sid = ensure_subscriber_id(conn, member)
                        if sid:
                            bc_send_flow(sid, FLOW_PENDENTE)
                        else:
                            print("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow pendente.", flush=True)

        except Exception as outer:
            print(f"💥 Loop erro: {outer}", flush=True)
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    work_loop()
