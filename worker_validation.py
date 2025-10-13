# -*- coding: utf-8 -*-
"""
Worker de validação:
- Consome validations_jobs
- Valida no site (consulta_medicos.buscar_sbcp)
- Atualiza membersnextlevel de forma tolerante (só colunas existentes)
- Ao final:
    * APROVADO  -> envia flow 7479824
    * PENDENTE/RECUSADO (FAILED) -> envia flow 7479965

Env:
- DATABASE_URL
- BOTCONVERSA_API_KEY (default: 362e173a-ba27-4655-9191-b4fd735394da)
- BOTCONVERSA_BASE_URL (default: https://backend.botconversa.com.br)
- BOTCONVERSA_FLOW_APROVADO (default: 7479824)
- BOTCONVERSA_FLOW_PENDENTE  (default: 7479965)
- POLL_SECONDS (default: 3)
- MAX_ATTEMPTS (default: 3)
"""

import os
import re
import time
import json
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp

# ------------------ Config ------------------
DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

BOTCONVERSA_API_KEY = os.getenv("BOTCONVERSA_API_KEY", "362e173a-ba27-4655-9191-b4fd735394da")
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
FLOW_APROVADO = int(os.getenv("BOTCONVERSA_FLOW_APROVADO", "7479824"))
FLOW_PENDENTE = int(os.getenv("BOTCONVERSA_FLOW_PENDENTE", "7479965"))

# ------------------ Utils ------------------
def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    if kwargs:
        msg += " " + " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(msg, flush=True)

def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def table_columns(conn, table: str, schema: str = "public") -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s""",
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}

# ------------------ Jobs table ops ------------------
def fetch_next_job(conn) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """SELECT id, member_id, email, nome, fonte, status, attempts
                 FROM validations_jobs
                WHERE status = 'PENDING'
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1"""
        )
        row = cur.fetchone()
        return dict(row) if row else None

def mark_running(conn, job_id: int, attempts: int) -> None:
    cols = table_columns(conn, "validations_jobs")
    sets, bind = [], []
    if "status" in cols:
        sets.append("status='RUNNING'")
    if "attempts" in cols:
        sets.append("attempts=%s")
        bind.append(attempts + 1)
    if "updated_at" in cols:
        sets.append("updated_at=NOW()")
    if not sets:
        return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE validations_jobs SET {', '.join(sets)} WHERE id=%s", (*bind, job_id))

def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    cols = table_columns(conn, "validations_jobs")
    sets, bind = [], []
    if "status" in cols:
        sets.append("status=%s")
        bind.append(status)
    if "last_error" in cols:
        sets.append("last_error=%s")
        bind.append(last_error)
    if "updated_at" in cols:
        sets.append("updated_at=NOW()")
    if not sets:
        return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE validations_jobs SET {', '.join(sets)} WHERE id=%s", (*bind, job_id))

# ------------------ Members ops ------------------
def get_member_core(conn, member_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, email, nome, metadata FROM membersnextlevel WHERE id=%s", (member_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def update_member_status(conn, member_id: int, status_txt: str, fonte: str):
    """Atualiza colunas somente se existirem."""
    cols = table_columns(conn, "membersnextlevel")
    sets, bind = [], []
    if "status_validation" in cols:
        sets.append("status_validation=%s")
        bind.append(status_txt)
    if "fonte_validation" in cols:
        sets.append("fonte_validation=%s")
        bind.append(fonte)
    if "updated_at" in cols:
        sets.append("updated_at=NOW()")
    if not sets:
        return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE membersnextlevel SET {', '.join(sets)} WHERE id=%s", (*bind, member_id))

def save_member_botconversa_id(conn, member_id: int, subscriber_id: int) -> None:
    cols = table_columns(conn, "membersnextlevel")
    if "metadata" not in cols:
        return
    sets = ["metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"]
    bind = [json.dumps({"botconversa_id": subscriber_id}, ensure_ascii=False)]
    if "updated_at" in cols:
        sets.append("updated_at=NOW()")
    with conn.cursor() as cur:
        cur.execute(f"UPDATE membersnextlevel SET {', '.join(sets)} WHERE id=%s", (*bind, member_id))

# ------------------ Document helpers ------------------
def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def split_number_uf(s: Optional[str]) -> Tuple[str, Optional[str]]:
    s = (s or "").strip().upper()
    if not s:
        return "", None
    m = re.search(r"^(.+?)(?:-|/|\s)([A-Z]{2})$", s)
    if m:
        return only_digits(m.group(1)), m.group(2)
    return only_digits(s), None

def pick_member_document(conn, member_id: int) -> str:
    """Busca doc esperado em metadata (doc/rqe/crm/crefito) e, se preciso, em raw_payload.data."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT metadata FROM membersnextlevel WHERE id=%s", (member_id,))
        row = cur.fetchone()
        meta = row["metadata"] if row else None
    if not isinstance(meta, dict):
        try:
            meta = json.loads(meta) if meta else {}
        except Exception:
            meta = {}
    lower = {str(k).lower(): v for k, v in (meta or {}).items()}
    for key in ["doc", "rqe", "crm", "crefito", "rqe_cirurgião", "rqe_cirurgiao"]:
        if key in lower and lower[key]:
            return str(lower[key]).strip()
    raw = lower.get("raw_payload")
    try:
        data = (raw.get("data") if isinstance(raw, dict) else None) or {}
        for k in list(data.keys()):
            kl = str(k).lower()
            if kl in ("rqe", "crm", "crefito"):
                val = str(data[k]).strip()
                if val:
                    return val
    except Exception:
        pass
    return ""

def collect_identifiers_from_result(result: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    def add(v: Optional[str]):
        if not v:
            return
        num, uf = split_number_uf(str(v))
        if not num:
            return
        ids.add(num)
        if uf:
            ids.add(f"{num}-{uf}")
    def add_list(lst):
        if not lst:
            return
        for v in lst:
            add(v)
    d = result.get("dados") or {}
    if isinstance(d, dict) and d:
        add(d.get("crm_padrao") or d.get("crm"))
        add(d.get("rqe_padrao") or d.get("rqe"))
        add(d.get("crefito_padrao") or d.get("crefito"))
        add_list(d.get("crms_padrao") or d.get("crms"))
        add_list(d.get("rqes_padrao") or d.get("rqes"))
        add_list(d.get("crefitos_padrao") or d.get("crefitos"))
    return ids

def match_document(expected: str, extracted_ids: Set[str]) -> bool:
    if not expected:
        return False
    num, uf = split_number_uf(expected)
    if uf and f"{num}-{uf}" in extracted_ids:
        return True
    return num in extracted_ids

# ------------------ BotConversa ------------------
def bc_headers() -> Dict[str, str]:
    return {"accept": "application/json", "Content-Type": "application/json", "API-KEY": BOTCONVERSA_API_KEY}

def bc_create_or_update_subscriber(phone: str, first_name: str, last_name: str) -> Optional[int]:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    payload = {"phone": phone, "first_name": first_name, "last_name": last_name}
    try:
        r = requests.post(url, headers=bc_headers(), json=payload, timeout=20)
        if not r.ok:
            log("❌ BotConversa subscriber FAIL", status=r.status_code, body=r.text)
            return None
        data = r.json()
        sid = data.get("id")
        try:
            return int(sid)
        except Exception:
            return None
    except Exception as e:
        log("❌ BotConversa subscriber EXC", err=repr(e))
        return None

def bc_send_flow(subscriber_id: int, flow_id: int) -> bool:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/{subscriber_id}/send_flow/"
    try:
        r = requests.post(url, headers=bc_headers(), json={"flow": int(flow_id)}, timeout=20)
        if not r.ok:
            log("❌ BotConversa send_flow FAIL", status=r.status_code, body=r.text)
        return bool(r.ok)
    except Exception as e:
        log("❌ BotConversa send_flow EXC", err=repr(e))
        return False

def split_person_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def ensure_subscriber_id(conn, member: Dict[str, Any]) -> Optional[int]:
    meta = member.get("metadata") or {}
    if not isinstance(meta, dict):
        try:
            meta = json.loads(meta) if meta else {}
        except Exception:
            meta = {}
    sid = meta.get("botconversa_id")
    if sid:
        try:
            return int(sid)
        except Exception:
            pass
    phone = (meta.get("phone") or "").strip()
    first_name, last_name = split_person_name(member.get("nome") or "")
    if not phone:
        return None
    sid = bc_create_or_update_subscriber(phone, first_name, last_name)
    if sid:
        save_member_botconversa_id(conn, member["id"], sid)
    return sid

def reset_stale_running(conn, minutes: int = 10) -> int:
    cols = table_columns(conn, "validations_jobs")
    if "status" not in cols:
        return 0
    with conn.cursor() as cur:
        if "updated_at" in cols:
            cur.execute(
                """
                UPDATE validations_jobs
                   SET status='PENDING', last_error='stale_running_reset', updated_at=NOW()
                 WHERE status='RUNNING'
                   AND COALESCE(updated_at, NOW() - INTERVAL '1 day') < NOW() - (%s || ' minutes')::interval
                """,
                (minutes,),
            )
        else:
            cur.execute("UPDATE validations_jobs SET status='PENDING' WHERE status='RUNNING'")
        return cur.rowcount or 0

# ------------------ Loop principal ------------------
def work_loop():
    conn = db()
    print("🚀 worker_validation iniciado", flush=True)
    freed = reset_stale_running(conn, minutes=10)
    if freed:
        print(f"🧹 destravados RUNNING: {freed}", flush=True)

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
                fonte = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    log(f"🧯 Job {job_id} -> FAILED (tentativas_excedidas)")
                    continue

                log(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id}]")
                mark_running(conn, job_id, attempts)

            steps: List[str] = []
            result: Dict[str, Any] = {}
            last_error: Optional[str] = None
            status_log: str = "init"

            # Documento esperado
            expected_doc = ""
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
                    steps.append(
                        f"ids_extraidos={sorted(list(extracted_ids))}" if extracted_ids else "ids_extraidos=vazio"
                    )
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

            # Atualiza status do membro (se colunas existirem)
            try:
                update_member_status(conn, member_id, "aprovado" if result.get("ok") else "pendente", fonte)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # Finalização + flows finais
            with conn:
                member = get_member_core(conn, member_id) or {"id": member_id, "nome": nome, "metadata": {}}

                if result.get("ok"):
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    log(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado)")
                    sid = ensure_subscriber_id(conn, member)
                    if sid:
                        bc_send_flow(sid, FLOW_APROVADO)
                    else:
                        log("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow aprovado.")
                else:
                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        log(f"🔁 Job {job_id} re-enfileirado (retry). status_log={status_log}")
                    else:
                        finalize_job(conn, job_id, "FAILED", last_error or status_log or "erro_definitivo")
                        log(f"🧯 Job {job_id} -> FAILED definitivo. status_log={status_log}")
                        sid = ensure_subscriber_id(conn, member)
                        if sid:
                            bc_send_flow(sid, FLOW_PENDENTE)
                        else:
                            log("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow pendente.")

        except Exception as outer:
            log(f"💥 Loop erro: {outer}")
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    work_loop()
