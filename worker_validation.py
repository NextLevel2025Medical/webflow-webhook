# -*- coding: utf-8 -*-
"""
Worker de validação:
- TTL (tempo máximo) por job: 2 minutos (configurável por TTL_SECONDS)
  * Re-enfileira RUNNING antigos (watchdog)
  * Se a execução passar de TTL, encerra por timeout (re-enfileira até MAX_ATTEMPTS; depois FAILED)
- Ao aprovar:
  * Envia flow aprovado no BotConversa
  * POST na Cademi para liberar conteúdo (codigo = <prefix> + job_id, email do membro)
- Escreve validations_log em toda finalização (se a tabela existir)

Env:
  DATABASE_URL
  BOTCONVERSA_API_KEY, BOTCONVERSA_BASE_URL
  BOTCONVERSA_FLOW_APROVADO=7479824, BOTCONVERSA_FLOW_PENDENTE=7479965
  CADEMI_URL, CADEMI_AUTH, CADEMI_PRODUTO_ID, CADEMI_TOKEN, CADEMI_CODIGO_PREFIX=LiberacaoIA
  TTL_SECONDS=120, MAX_ATTEMPTS=3, POLL_SECONDS=3
"""
import os, re, time, json
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp

DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
TTL_SECONDS  = int(os.getenv("TTL_SECONDS", "120"))  # 2 minutos

BOTCONVERSA_API_KEY  = os.getenv("BOTCONVERSA_API_KEY", "362e173a-ba27-4655-9191-b4fd735394da")
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
FLOW_APROVADO        = int(os.getenv("BOTCONVERSA_FLOW_APROVADO", "7479824"))
FLOW_PENDENTE        = int(os.getenv("BOTCONVERSA_FLOW_PENDENTE", "7479965"))

# Cademi
CADEMI_URL          = os.getenv("CADEMI_URL", "https://nextlevelmedical.cademi.com.br/api/postback/custom")
CADEMI_AUTH         = os.getenv("CADEMI_AUTH", "e633cefa-b72a-4214-a56f-fd71a39576dd")
CADEMI_PRODUTO_ID   = os.getenv("CADEMI_PRODUTO_ID", "plastic-transicao")
CADEMI_TOKEN        = os.getenv("CADEMI_TOKEN", "6e88c3b468378317d758f5f1c09cd2ec")
CADEMI_CODIGO_PREF  = os.getenv("CADEMI_CODIGO_PREFIX", "LiberacaoIA")  # use 'LiberaçãoIA' se preferir com acento

def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    if kwargs: msg += " " + " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(msg, flush=True)

def db():
    if not DATABASE_URL: raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL); conn.autocommit = True; return conn

def table_columns(conn, table: str, schema: str = "public") -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s""", (schema, table))
        return {r[0] for r in cur.fetchall()}

def get_tables(conn) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public'""")
        return {r[0] for r in cur.fetchall()}

# ---------- jobs ----------
def fetch_next_job(conn) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """SELECT id, member_id, email, nome, fonte, status, attempts
                 FROM validations_jobs
                WHERE status='PENDING'
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1"""
        )
        row = cur.fetchone()
        return dict(row) if row else None

def mark_running(conn, job_id: int, attempts: int) -> None:
    cols = table_columns(conn, "validations_jobs")
    sets, bind = [], []
    if "status" in cols:    sets.append("status='RUNNING'")
    if "attempts" in cols:  sets.append("attempts=%s"); bind.append(attempts + 1)
    if "updated_at" in cols:sets.append("updated_at=NOW()")
    if "started_at" in cols:sets.append("started_at=NOW()")
    if not sets: return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE validations_jobs SET {', '.join(sets)} WHERE id=%s", (*bind, job_id))

def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    cols = table_columns(conn, "validations_jobs")
    sets, bind = [], []
    if "status" in cols:     sets.append("status=%s"); bind.append(status)
    if "last_error" in cols: sets.append("last_error=%s"); bind.append(last_error)
    if "updated_at" in cols: sets.append("updated_at=NOW()")
    with conn.cursor() as cur:
        cur.execute(f"UPDATE validations_jobs SET {', '.join(sets)} WHERE id=%s", (*bind, job_id))

def requeue_stale_running_jobs(conn, ttl_seconds: int) -> int:
    """Re-enfileira RUNNING muito antigos (watchdog)."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE validations_jobs
               SET status='PENDING',
                   last_error='ttl_requeue',
                   updated_at=NOW()
             WHERE status='RUNNING'
               AND updated_at < NOW() - INTERVAL '{int(ttl_seconds)} seconds'
            """
        )
        return cur.rowcount or 0

# ---------- members ----------
def get_member_core(conn, member_id: int) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, email, nome, metadata FROM membersnextlevel WHERE id=%s", (member_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_phone_by_member(conn, member_id: int) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT metadata->>'phone' FROM membersnextlevel WHERE id=%s", (member_id,))
        row = cur.fetchone()
        return (row[0] or "") if row else ""

def update_member_after_result(conn, member_id: int, fonte: str, result: Dict[str, Any], expected_doc: str):
    cols = table_columns(conn, "membersnextlevel")
    sets, bind = [], []
    status_txt = "aprovado" if result.get("ok") else "pendente"
    if "validacao_acesso" in cols: sets.append("validacao_acesso=%s"); bind.append(status_txt)
    if "portal_validado" in cols:  sets.append("portal_validado=%s"); bind.append(fonte)
    if "validacao_at" in cols:     sets.append("validacao_at=NOW()")
    dados = result.get("dados") or {}
    doc_val = dados.get("rqe_padrao") or dados.get("crm_padrao") or expected_doc or None
    if "doc" in cols and doc_val: sets.append("doc=%s"); bind.append(doc_val)
    if "rqe" in cols and dados.get("rqe_padrao"): sets.append("rqe=%s"); bind.append(dados.get("rqe_padrao"))
    if "crm" in cols and dados.get("crm_padrao"): sets.append("crm=%s"); bind.append(dados.get("crm_padrao"))
    if "crefito" in cols and dados.get("crefito_padrao"): sets.append("crefito=%s"); bind.append(dados.get("crefito_padrao"))
    if "metadata" in cols:
        patch = json.dumps({"validation_result": result}, ensure_ascii=False)
        sets.append("metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"); bind.append(patch)
    if "updated_at" in cols: sets.append("updated_at=NOW()")
    if not sets: return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE membersnextlevel SET {', '.join(sets)} WHERE id=%s", (*bind, member_id))

# ---------- logs ----------
def insert_validation_log(conn, member_id: int, fonte: str, status_txt: str, payload: Dict[str, Any]):
    if "validations_log" not in get_tables(conn): return
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO validations_log (member_id, fonte, status, payload, created_at) VALUES (%s,%s,%s,%s::jsonb,NOW())",
            (member_id, fonte, status_txt, json.dumps({"raw": payload}, ensure_ascii=False)),
        )

# ---------- documento helpers ----------
def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def split_number_uf(s: Optional[str]) -> Tuple[str, Optional[str]]:
    s = (s or "").strip().upper()
    if not s: return "", None
    m = re.search(r"^(.+?)(?:-|/|\s)([A-Z]{2})$", s)
    if m: return only_digits(m.group(1)), m.group(2)
    return only_digits(s), None

def _safe_lower_dict(d: Any) -> Dict[str, Any]:
    if not isinstance(d, dict): return {}
    return {str(k).lower(): v for k, v in d.items()}

def _extract_data_from_raw_payload(raw_payload: Any) -> Dict[str, Any]:
    if not isinstance(raw_payload, dict):
        try: raw_payload = json.loads(raw_payload) if raw_payload else {}
        except Exception: raw_payload = {}
    if isinstance(raw_payload.get("data"), dict): return _safe_lower_dict(raw_payload["data"])
    if isinstance(raw_payload.get("payload"), dict) and isinstance(raw_payload["payload"].get("data"), dict):
        return _safe_lower_dict(raw_payload["payload"]["data"])
    return {}

def pick_member_document(conn, member_id: int) -> str:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT metadata FROM membersnextlevel WHERE id=%s", (member_id,))
        row = cur.fetchone()
        meta = row["metadata"] if row else None
    if not isinstance(meta, dict):
        try: meta = json.loads(meta) if meta else {}
        except Exception: meta = {}
    lower = _safe_lower_dict(meta)
    for key in ["doc","rqe","crm","crefito","rqe_cirurgião","rqe_cirurgiao"]:
        if key in lower and lower[key]:
            return str(lower[key]).strip()
    data = _extract_data_from_raw_payload(lower.get("raw_payload"))
    for key in ["rqe","crm","crefito"]:
        val = data.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return ""

def collect_identifiers_from_result(result: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    def add(v: Optional[str]):
        if not v: return
        num, uf = split_number_uf(str(v))
        if not num: return
        ids.add(num)
        if uf: ids.add(f"{num}-{uf}")
    def add_list(lst):
        if not lst: return
        for v in lst: add(v)
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
    if not expected: return False
    num, uf = split_number_uf(expected)
    if uf and f"{num}-{uf}" in extracted_ids: return True
    return num in extracted_ids

# ---------- BotConversa ----------
def bc_headers() -> Dict[str, str]:
    return {"accept":"application/json","Content-Type":"application/json","API-KEY":BOTCONVERSA_API_KEY}

def bc_create_or_update_subscriber(phone: str, first_name: str, last_name: str) -> Optional[int]:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    try:
        r = requests.post(url, headers=bc_headers(), json={"phone":phone,"first_name":first_name,"last_name":str(last_name or "")}, timeout=20)
        if not r.ok: log("❌ BotConversa subscriber FAIL", status=r.status_code, body=r.text); return None
        data = r.json(); sid = data.get("id")
        try: return int(sid)
        except Exception: return None
    except Exception as e:
        log("❌ BotConversa subscriber EXC", err=repr(e)); return None

def bc_send_flow(subscriber_id: int, flow_id: int) -> bool:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/{subscriber_id}/send_flow/"
    try:
        r = requests.post(url, headers=bc_headers(), json={"flow":int(flow_id)}, timeout=20)
        if not r.ok: log("❌ BotConversa send_flow FAIL", status=r.status_code, body=r.text)
        return bool(r.ok)
    except Exception as e:
        log("❌ BotConversa send_flow EXC", err=repr(e)); return False

def split_person_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])

def ensure_subscriber_id(conn, member: Dict[str, Any]) -> Optional[int]:
    meta = member.get("metadata") or {}
    if not isinstance(meta, dict):
        try: meta = json.loads(meta) if meta else {}
        except Exception: meta = {}
    sid = meta.get("botconversa_id")
    if sid:
        try: return int(sid)
        except Exception: pass
    phone = (meta.get("phone") or "").strip()
    first_name, last_name = split_person_name(member.get("nome") or "")
    if not phone: return None
    sid = bc_create_or_update_subscriber(phone, first_name, last_name)
    if sid: save_member_botconversa_id(conn, member["id"], sid)
    return sid

def save_member_botconversa_id(conn, member_id: int, subscriber_id: int) -> None:
    cols = table_columns(conn, "membersnextlevel")
    if "metadata" not in cols: return
    sets = ["metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"]; bind = [json.dumps({"botconversa_id": subscriber_id}, ensure_ascii=False)]
    if "updated_at" in cols: sets.append("updated_at=NOW()")
    with conn.cursor() as cur:
        cur.execute(f"UPDATE membersnextlevel SET {', '.join(sets)} WHERE id=%s", (*bind, member_id))

# ---------- coordenação por telefone ----------
def cancel_other_jobs_for_phone(conn, phone: str, exclude_job_id: int) -> int:
    if not phone: return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validations_jobs j
               SET status='CANCELLED', last_error='cancelled_by_other_success', updated_at=NOW()
              FROM membersnextlevel m
             WHERE j.member_id = m.id
               AND m.metadata->>'phone' = %s
               AND j.id <> %s
               AND j.status IN ('PENDING','RUNNING')
            """,
            (phone, exclude_job_id),
        )
        return cur.rowcount or 0

def exists_succeeded_for_phone(conn, phone: str) -> bool:
    if not phone: return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM validations_jobs j
              JOIN membersnextlevel m ON m.id = j.member_id
             WHERE m.metadata->>'phone' = %s
               AND j.status = 'SUCCEEDED'
             LIMIT 1
            """,
            (phone,),
        )
        return cur.fetchone() is not None

# ---------- Cademi ----------
def cademi_headers() -> Dict[str, str]:
    return {"Authorization": CADEMI_AUTH, "Content-Type": "application/json"}

def cademi_postback(job_id: int, cliente_email: str) -> bool:
    payload = {
        "codigo": f"{CADEMI_CODIGO_PREF}{job_id}",
        "status": "aprovado",
        "produto_id": str(CADEMI_PRODUTO_ID),
        "cliente_email": cliente_email,
        "token": CADEMI_TOKEN,
    }
    try:
        r = requests.post(CADEMI_URL, headers=cademi_headers(), json=payload, timeout=30)
        ok = bool(r.ok)
        if not ok:
            log("❌ Cademi FAIL", status=r.status_code, body=r.text)
        else:
            log("✅ Cademi OK", job_id=job_id, email=cliente_email)
        return ok
    except Exception as e:
        log("❌ Cademi EXC", err=repr(e)); return False

# ---------- loop ----------
def work_loop():
    conn = db()
    print("🚀 worker_validation iniciado", flush=True)

    while True:
        try:
            # watchdog: limpa RUNNING antigos
            stale = requeue_stale_running_jobs(conn, TTL_SECONDS)
            if stale:
                log(f"⏱️  Watchdog re-enfileirou {stale} job(s) RUNNING > {TTL_SECONDS}s")

            with conn:
                job = fetch_next_job(conn)
                if not job:
                    time.sleep(POLL_SECONDS); continue

                job_id   = job["id"]
                member_id= job["member_id"]
                email    = job.get("email") or ""
                nome     = job.get("nome") or ""
                fonte    = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    insert_validation_log(conn, member_id, fonte, "tentativas_excedidas", {"job_id": job_id})
                    log(f"🧯 Job {job_id} -> FAILED (tentativas_excedidas)"); continue

                log(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id}]")
                mark_running(conn, job_id, attempts)

            start = time.monotonic()
            steps: List[str] = []
            result: Dict[str, Any] = {}
            last_error: Optional[str] = None
            status_log: str = "init"

            expected_doc = ""
            try:
                expected_doc = pick_member_document(conn, member_id).strip()
                if not expected_doc:
                    status_log = "sem_documento"
                    result = {"ok": False, "reason": "documento_vazio", "steps": steps + ["documento_vazio"]}
                else:
                    steps.append(f"expected_doc={expected_doc}")
            except Exception as e:
                last_error = f"db_erro:{e}"; status_log = "db_erro"; result = {"ok": False, "reason": "db_erro", "steps": steps}

            if result.get("reason") != "documento_vazio" and not last_error:
                try:
                    result = buscar_sbcp(member_id=member_id, nome=nome, email=email, steps=steps)
                    status_log = "executado"
                except Exception as e:
                    last_error = f"exec_erro:{e}"; status_log = "error_execucao"
                    result = {"ok": False, "reason": "error_execucao", "steps": steps}

            # matching
            if not last_error and result and result.get("reason") != "documento_vazio":
                try:
                    extracted_ids = collect_identifiers_from_result(result)
                    steps.append(f"ids_extraidos={sorted(list(extracted_ids))}" if extracted_ids else "ids_extraidos=vazio")
                    is_match = match_document(expected_doc, extracted_ids)
                    result["expected_doc"] = expected_doc
                    result["match"] = bool(is_match)
                    if result.get("reason") == "sem_resultados_ou_layout_alterado":
                        status_log = "sem_resultados"; result["ok"] = False
                    elif is_match:
                        status_log = "ok"; result["ok"] = True
                    else:
                        status_log = "numero_registro_invalido"; result["ok"] = False
                except Exception as e:
                    last_error = f"match_erro:{e}"; status_log = "match_erro"; result["ok"] = False

            elapsed = time.monotonic() - start
            timed_out = elapsed > TTL_SECONDS

            # Atualiza o "banco principal"
            try:
                update_member_after_result(conn, member_id, fonte, result, expected_doc)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # Finalização + fluxos + Cademi + logs
            with conn:
                member = get_member_core(conn, member_id) or {"id": member_id, "nome": nome, "metadata": {}}
                phone  = get_phone_by_member(conn, member_id)

                if (result.get("ok") and not timed_out):
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    insert_validation_log(conn, member_id, fonte, "ok", result)
                    log(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado)")

                    # Cancela outros jobs do mesmo telefone
                    cancelled = cancel_other_jobs_for_phone(conn, phone, job_id)
                    if cancelled: log(f"🧹 Cancelados {cancelled} job(s) antigos para phone={phone}")

                    # Flow aprovado
                    sid = ensure_subscriber_id(conn, member)
                    if sid: bc_send_flow(sid, FLOW_APROVADO)
                    else: log("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow aprovado.")

                    # CADEMI – liberação de conteúdo
                    if email:
                        cademi_postback(job_id, email)
                    else:
                        log("⚠️ Cademi: e-mail vazio; liberação não enviada.")

                else:
                    # timeout tem prioridade de mensagem
                    if timed_out:
                        last_error = (last_error or "") + ("; " if last_error else "") + "timeout_ttl"
                        status_log = "timeout_ttl"

                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        insert_validation_log(conn, member_id, fonte, status_log or "retry", result or {"elapsed": elapsed})
                        log(f"🔁 Job {job_id} re-enfileirado (retry). status_log={status_log}")
                    else:
                        finalize_job(conn, job_id, "FAILED", last_error or status_log or "erro_definitivo")
                        insert_validation_log(conn, member_id, fonte, status_log or "failed", result or {"elapsed": elapsed})
                        log(f"🧯 Job {job_id} -> FAILED definitivo. status_log={status_log}")
                        if not exists_succeeded_for_phone(conn, phone):
                            sid = ensure_subscriber_id(conn, member)
                            if sid: bc_send_flow(sid, FLOW_PENDENTE)
                            else: log("⚠️ BotConversa: subscriber_id ausente; não foi possível enviar flow pendente.")
                        else:
                            log(f"🚫 Pendente suprimido: já existe SUCCEEDED para phone={phone}")

        except Exception as outer:
            log(f"💥 Loop erro: {outer}")
            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    work_loop()

