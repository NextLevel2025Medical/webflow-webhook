# -*- coding: utf-8 -*-
import os, re, json
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, List, Set

import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

DATABASE_URL = os.getenv("DATABASE_URL")
BOTCONVERSA_API_KEY = os.getenv("BOTCONVERSA_API_KEY", "362e173a-ba27-4655-9191-b4fd735394da")
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
BOTCONVERSA_FLOW_ANALISE = int(os.getenv("BOTCONVERSA_FLOW_ANALISE", "7479821"))
DEFAULT_COUNTRY_ISO = os.getenv("DEFAULT_COUNTRY_ISO", "BR").upper().strip()
SERVICE_PORT = int(os.getenv("PORT", os.getenv("SERVICE_PORT", "10000")))

app = Flask(__name__)

def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    if kwargs:
        msg += " " + " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(msg, flush=True)

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def normalize_phone_br(phone: str) -> str:
    digits = only_digits(phone)
    if not digits: return ""
    if digits.startswith("55") and len(digits) >= 12: return digits
    if DEFAULT_COUNTRY_ISO == "BR" and 10 <= len(digits) <= 11: return f"55{digits}"
    return digits

def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts: return "Visitante", ""
    if len(parts) == 1: return parts[0], ""
    return parts[0], " ".join(parts[1:])

def lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in (d or {}).items()}

def first_present(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return str(d[k]).strip()
    return None

def extract_original_json() -> Dict[str, Any]:
    j = request.get_json(silent=True)
    return j if isinstance(j, dict) else {}

def get_form_data_block(original: Dict[str, Any]) -> Dict[str, Any]:
    base = original
    if isinstance(original.get("payload"), dict):
        base = original["payload"]
    data = base.get("data") if isinstance(base.get("data"), dict) else {}
    out = lower_keys(data)
    if isinstance(data.get("form"), dict):
        form_lower = lower_keys(data["form"])
        for k, v in form_lower.items():
            out.setdefault(k, v)
    return out

def extract_doc_from_data(form_data: Dict[str, Any]) -> Optional[str]:
    for key in ["rqe", "crm", "crefito"]:
        val = form_data.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return None

def bc_headers() -> Dict[str, str]:
    return {"accept": "application/json", "Content-Type": "application/json", "API-KEY": BOTCONVERSA_API_KEY}

def bc_create_or_update_subscriber(phone_digits: str, first_name: str, last_name: str) -> Optional[int]:
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    payload = {"phone": phone_digits, "first_name": first_name, "last_name": last_name}
    try:
        r = requests.post(url, headers=bc_headers(), json=payload, timeout=20)
        if not r.ok:
            log("❌ BotConversa subscriber FAIL", status=r.status_code, body=r.text)
            return None
        data = r.json()
        sid = data.get("id")
        try: return int(sid)
        except Exception: return None
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

def table_columns(conn, table: str, schema: str = "public") -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s""",
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}

def has_unique_on_email(conn, table: str = "membersnextlevel", schema: str = "public") -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = %s
               AND tc.table_name   = %s
               AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY')
               AND kcu.column_name = 'email'
             LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None

def upsert_member(conn, email: str, nome: str, phone_digits: str, raw_payload: Dict[str, Any]) -> int:
    cols = table_columns(conn, "membersnextlevel")
    has_created = "created_at" in cols
    has_updated = "updated_at" in cols
    has_metadata = "metadata" in cols
    has_doc_col = "doc" in cols
    has_rqe_col = "rqe" in cols
    has_crm_col = "crm" in cols
    has_crefito_col = "crefito" in cols

    form_data = get_form_data_block(raw_payload)
    doc_hint = extract_doc_from_data(form_data)
    meta_obj = {"phone": phone_digits, "raw_payload": raw_payload}
    if doc_hint:
        meta_obj["doc"] = doc_hint
        if "rqe" in form_data: meta_obj["rqe"] = form_data["rqe"]
        if "crm" in form_data: meta_obj["crm"] = form_data["crm"]
        if "crefito" in form_data: meta_obj["crefito"] = form_data["crefito"]
    meta_json = json.dumps(meta_obj, ensure_ascii=False)

    if has_unique_on_email(conn):
        insert_cols = ["email", "nome"]
        insert_vals = ["%s", "%s"]
        bind = [email, nome]
        if has_metadata:
            insert_cols += ["metadata"]; insert_vals += ["%s::jsonb"]; bind += [meta_json]
        if has_doc_col and doc_hint:
            insert_cols += ["doc"]; insert_vals += ["%s"]; bind += [doc_hint]
        if has_rqe_col and form_data.get("rqe"):
            insert_cols += ["rqe"]; insert_vals += ["%s"]; bind += [form_data.get("rqe")]
        if has_crm_col and form_data.get("crm"):
            insert_cols += ["crm"]; insert_vals += ["%s"]; bind += [form_data.get("crm")]
        if has_crefito_col and form_data.get("crefito"):
            insert_cols += ["crefito"]; insert_vals += ["%s"]; bind += [form_data.get("crefito")]
        if has_created: insert_cols += ["created_at"]; insert_vals += ["NOW()"]
        if has_updated: insert_cols += ["updated_at"]; insert_vals += ["NOW()"]

        set_parts = ["nome = EXCLUDED.nome"]
        if has_metadata:
            set_parts.append("metadata = COALESCE(membersnextlevel.metadata,'{}'::jsonb) || EXCLUDED.metadata")
        if has_doc_col and doc_hint:
            set_parts.append("doc = COALESCE(EXCLUDED.doc, membersnextlevel.doc)")
        if has_rqe_col and form_data.get("rqe"):
            set_parts.append("rqe = COALESCE(EXCLUDED.rqe, membersnextlevel.rqe)")
        if has_crm_col and form_data.get("crm"):
            set_parts.append("crm = COALESCE(EXCLUDED.crm, membersnextlevel.crm)")
        if has_crefito_col and form_data.get("crefito"):
            set_parts.append("crefito = COALESCE(EXCLUDED.crefito, membersnextlevel.crefito)")
        if has_updated: set_parts.append("updated_at = NOW()")

        sql = f"""INSERT INTO membersnextlevel ({", ".join(insert_cols)})
                  VALUES ({", ".join(insert_vals)})
                  ON CONFLICT (email) DO UPDATE SET {", ".join(set_parts)}
                  RETURNING id"""
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, bind)
            row = cur.fetchone()
            mid = int(row["id"]); log("👤 UPSERT member", email=email, id=mid)
            return mid

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id FROM membersnextlevel WHERE email=%s LIMIT 1", (email,))
        row = cur.fetchone()
        if row:
            mid = int(row["id"])
            set_parts = ["nome=%s"]; bind2 = [nome]
            if has_metadata:
                set_parts.append("metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"); bind2.append(meta_json)
            if has_doc_col and doc_hint:
                set_parts.append("doc = COALESCE(%s, doc)"); bind2.append(doc_hint)
            if has_rqe_col and form_data.get("rqe"):
                set_parts.append("rqe = COALESCE(%s, rqe)"); bind2.append(form_data.get("rqe"))
            if has_crm_col and form_data.get("crm"):
                set_parts.append("crm = COALESCE(%s, crm)"); bind2.append(form_data.get("crm"))
            if has_crefito_col and form_data.get("crefito"):
                set_parts.append("crefito = COALESCE(%s, crefito)"); bind2.append(form_data.get("crefito"))
            if has_updated: set_parts.append("updated_at = NOW()")
            cur.execute(f"UPDATE membersnextlevel SET {', '.join(set_parts)} WHERE id=%s", (*bind2, mid))
            log("👤 UPDATE member", email=email, id=mid); return mid

        insert_cols = ["email", "nome"]; insert_vals = ["%s", "%s"]; bind3 = [email, nome]
        if has_metadata: insert_cols += ["metadata"]; insert_vals += ["%s::jsonb"]; bind3 += [meta_json]
        if has_doc_col and doc_hint: insert_cols += ["doc"]; insert_vals += ["%s"]; bind3 += [doc_hint]
        if has_rqe_col and form_data.get("rqe"): insert_cols += ["rqe"]; insert_vals += ["%s"]; bind3 += [form_data.get("rqe")]
        if has_crm_col and form_data.get("crm"): insert_cols += ["crm"]; insert_vals += ["%s"]; bind3 += [form_data.get("crm")]
        if has_crefito_col and form_data.get("crefito"): insert_cols += ["crefito"]; insert_vals += ["%s"]; bind3 += [form_data.get("crefito")]
        if has_created: insert_cols += ["created_at"]; insert_vals += ["NOW()"]
        if has_updated: insert_cols += ["updated_at"]; insert_vals += ["NOW()"]
        cur.execute(
            f"INSERT INTO membersnextlevel ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)}) RETURNING id",
            bind3,
        )
        row = cur.fetchone(); mid = int(row["id"])
        log("👤 INSERT member", email=email, id=mid); return mid

def save_botconversa_id(conn, member_id: int, subscriber_id: int):
    cols = table_columns(conn, "membersnextlevel")
    if "metadata" not in cols: return
    set_parts = ["metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"]
    bind = [json.dumps({"botconversa_id": subscriber_id}, ensure_ascii=False)]
    if "updated_at" in cols: set_parts.append("updated_at = NOW()")
    with conn.cursor() as cur:
        cur.execute(f"UPDATE membersnextlevel SET {', '.join(set_parts)} WHERE id=%s", (*bind, member_id))

def enqueue_validation_job(conn, member_id: int, email: str, nome: str, fonte: str = "sbcp"):
    cols = table_columns(conn, "validations_jobs")
    insert_cols = ["member_id", "email", "nome"]; insert_vals = ["%s", "%s", "%s"]; bind = [member_id, email, nome]
    if "fonte" in cols: insert_cols.append("fonte"); insert_vals.append("%s"); bind.append(fonte)
    if "status" in cols: insert_cols.append("status"); insert_vals.append("%s"); bind.append("PENDING")
    if "attempts" in cols: insert_cols.append("attempts"); insert_vals.append("%s"); bind.append(0)
    if "created_at" in cols: insert_cols.append("created_at"); insert_vals.append("NOW()")
    if "updated_at" in cols: insert_cols.append("updated_at"); insert_vals.append("NOW()")
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO validations_jobs ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals)})", bind)
    log("📥 Job enfileirado", member_id=member_id, email=email, fonte=fonte, status="PENDING")

def parse_fields_from_payload() -> Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]:
    warns: Dict[str, Any] = {}
    original = extract_original_json()
    form = get_form_data_block(original)
    email = first_present(form, ["email", "e-mail", "e_mail", "mail"]) or ""
    full_name = first_present(form, ["nome", "full_name", "fullname", "full name"]) or ""
    phone = first_present(form, ["celular", "whatsapp", "phone", "telefone", "tel", "mobile"]) or ""
    phone_digits = normalize_phone_br(phone)
    if not full_name: full_name = "Visitante"; warns["no_name"] = True
    if not email:
        if phone_digits:
            email = f"{phone_digits}@temp.nextlevelmedical.local"; warns["email_fallback"] = "from_phone"
        else:
            email = f"lead-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}@temp.nextlevelmedical.local"; warns["email_fallback"] = "timestamp"
    if not phone_digits and phone: warns["bad_phone_format"] = phone
    meta_extra = {"raw_payload": original}
    return email, full_name, phone_digits, meta_extra, warns

@app.get("/")
def index():
    return jsonify({"ok": True, "service": "webflow-webhook", "ts": datetime.utcnow().isoformat()}), 200

@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat()}), 200

@app.post("/webflow-webhook")
def webflow_webhook():
    conn = None
    try:
        email, full_name, phone_digits, extra_meta, warns = parse_fields_from_payload()
        first_name, last_name = split_name(full_name)
        conn = db()
        member_id = upsert_member(conn, email=email, nome=full_name, phone_digits=phone_digits, raw_payload=extra_meta.get("raw_payload", {}))

        subscriber_id = None
        if phone_digits:
            subscriber_id = bc_create_or_update_subscriber(phone_digits, first_name, last_name)
            if subscriber_id: save_botconversa_id(conn, member_id, subscriber_id)
        if subscriber_id: bc_send_flow(subscriber_id, BOTCONVERSA_FLOW_ANALISE)

        enqueue_validation_job(conn, member_id=member_id, email=email, nome=full_name, fonte="sbcp")

        resp = {"ok": True, "member_id": member_id, "subscriber_id": subscriber_id, "flow_id": BOTCONVERSA_FLOW_ANALISE if subscriber_id else None}
        if warns: resp["warn"] = warns
        return jsonify(resp), 200

    except Exception as e:
        if conn:
            try: conn.rollback()
            except Exception: pass
        log("💥 webhook_error", err=repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if conn:
            try: conn.close()
            except Exception: pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=SERVICE_PORT)
