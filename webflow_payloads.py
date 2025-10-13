# -*- coding: utf-8 -*-
"""
Webhook (Webflow -> NextLevel) robusto e tolerante, com integração BotConversa.

- Envia o FLOW "em análise" no cadastro (default: 7479821; override via BOTCONVERSA_FLOW_ANALISE).
- Tolerante a payloads (JSON e form), nome/email/telefone ausentes.
- Compatível com schemas SEM created_at/updated_at/metadata.
- NÃO usa psycopg2.sql (evita AttributeError).

Env:
- DATABASE_URL
- BOTCONVERSA_API_KEY        (default: 362e173a-ba27-4655-9191-b4fd735394da)
- BOTCONVERSA_BASE_URL       (default: https://backend.botconversa.com.br)
- BOTCONVERSA_FLOW_ANALISE   (default: 7479821)
- DEFAULT_COUNTRY_ISO        (default: BR)
- PORT / SERVICE_PORT        (default: 10000)
"""

import os
import re
import json
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, List, Set

import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

# -------------------- Config --------------------
DATABASE_URL = os.getenv("DATABASE_URL")

BOTCONVERSA_API_KEY = os.getenv(
    "BOTCONVERSA_API_KEY",
    "362e173a-ba27-4655-9191-b4fd735394da"
)
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
BOTCONVERSA_FLOW_ANALISE = int(os.getenv("BOTCONVERSA_FLOW_ANALISE", "7479821"))

DEFAULT_COUNTRY_ISO = os.getenv("DEFAULT_COUNTRY_ISO", "BR").upper().strip()
SERVICE_PORT = int(os.getenv("PORT", os.getenv("SERVICE_PORT", "10000")))

app = Flask(__name__)

# -------------------- Utils --------------------
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

def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def only_digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def normalize_phone_br(phone: str) -> str:
    digits = only_digits(phone)
    if not digits:
        return ""
    if digits.startswith("55") and len(digits) >= 12:
        return digits
    if DEFAULT_COUNTRY_ISO == "BR" and 10 <= len(digits) <= 11:
        return f"55{digits}"
    return digits

def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "Visitante", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in d.items()}

def first_present(d: Dict[str, Any], *keys) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return str(d[k]).strip()
    return None

def coalesce_payload() -> Dict[str, Any]:
    base: Dict[str, Any] = {}

    j = request.get_json(silent=True)
    if isinstance(j, dict):
        base.update(j)

    if request.form:
        for k in request.form:
            base.setdefault(k, request.form.get(k))

    if request.args:
        for k in request.args:
            base.setdefault(k, request.args.get(k))

    # Camada "data" / "form" frequente em Webflow/Zapier
    if isinstance(base.get("data"), dict):
        data = base["data"].copy()
        if isinstance(data.get("form"), dict):
            nested = {**data, **data["form"]}
            nested.pop("form", None)
            return lower_keys(nested)
        return lower_keys(data)

    if isinstance(base.get("payload"), dict):
        return lower_keys(base["payload"])

    return lower_keys(base)

# -------------------- DB Introspection --------------------
def table_columns(conn, table: str, schema: str = "public") -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = %s
               AND table_name = %s
            """,
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
               AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
               AND kcu.column_name = 'email'
             LIMIT 1
            """,
            (schema, table),
        )
        return cur.fetchone() is not None

# -------------------- BotConversa --------------------
def bc_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Content-Type": "application/json",
        "API-KEY": BOTCONVERSA_API_KEY,
    }

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
        ok = bool(r.ok)
        if not ok:
            log("❌ BotConversa send_flow FAIL", status=r.status_code, body=r.text)
        return ok
    except Exception as e:
        log("❌ BotConversa send_flow EXC", err=repr(e))
        return False

# -------------------- DB Ops (sem psycopg2.sql) --------------------
def upsert_member(conn, email: str, nome: str, phone_digits: str, extra_meta: Dict[str, Any]) -> int:
    cols = table_columns(conn, "membersnextlevel")
    has_created = "created_at" in cols
    has_updated = "updated_at" in cols
    has_metadata = "metadata" in cols

    meta_json_str = json.dumps({"phone": phone_digits, **(extra_meta or {})}, ensure_ascii=False)

    # ON CONFLICT se houver unique em email
    if has_unique_on_email(conn):
        insert_cols = ["email", "nome"]
        insert_vals_placeholders = ["%s", "%s"]
        bind_vals: List[Any] = [email, nome]

        if has_metadata:
            insert_cols.append("metadata")
            insert_vals_placeholders.append("%s::jsonb")
            bind_vals.append(meta_json_str)
        if has_created:
            insert_cols.append("created_at")
            insert_vals_placeholders.append("NOW()")
        if has_updated:
            insert_cols.append("updated_at")
            insert_vals_placeholders.append("NOW()")

        set_parts = ["nome = EXCLUDED.nome"]
        if has_metadata:
            set_parts.append("metadata = COALESCE(membersnextlevel.metadata, '{}'::jsonb) || EXCLUDED.metadata")
        if has_updated:
            set_parts.append("updated_at = NOW()")
        set_sql = ", ".join(set_parts)

        sql = f"""
            INSERT INTO membersnextlevel ({", ".join(insert_cols)})
            VALUES ({", ".join(insert_vals_placeholders)})
            ON CONFLICT (email) DO UPDATE
               SET {set_sql}
            RETURNING id
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, bind_vals)
            row = cur.fetchone()
            mid = int(row["id"])
            log("👤 UPSERT member", email=email, id=mid)
            return mid

    # Fallback: SELECT/UPDATE/INSERT
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id FROM membersnextlevel WHERE email=%s LIMIT 1", (email,))
        row = cur.fetchone()
        if row:
            mid = int(row["id"])
            set_parts = ["nome=%s"]
            bind_vals2: List[Any] = [nome]

            if has_metadata:
                set_parts.append("metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb")
                bind_vals2.append(meta_json_str)
            if has_updated:
                set_parts.append("updated_at = NOW()")

            cur.execute(
                f"UPDATE membersnextlevel SET {', '.join(set_parts)} WHERE id=%s",
                (*bind_vals2, mid),
            )
            log("👤 UPDATE member", email=email, id=mid)
            return mid

        # INSERT
        insert_cols = ["email", "nome"]
        insert_vals_placeholders = ["%s", "%s"]
        bind_vals3: List[Any] = [email, nome]

        if has_metadata:
            insert_cols.append("metadata")
            insert_vals_placeholders.append("%s::jsonb")
            bind_vals3.append(meta_json_str)
        if has_created:
            insert_cols.append("created_at")
            insert_vals_placeholders.append("NOW()")
        if has_updated:
            insert_cols.append("updated_at")
            insert_vals_placeholders.append("NOW()")

        cur.execute(
            f"INSERT INTO membersnextlevel ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals_placeholders)}) RETURNING id",
            bind_vals3,
        )
        row = cur.fetchone()
        mid = int(row["id"])
        log("👤 INSERT member", email=email, id=mid)
        return mid

def save_botconversa_id(conn, member_id: int, subscriber_id: int):
    cols = table_columns(conn, "membersnextlevel")
    has_metadata = "metadata" in cols
    has_updated = "updated_at" in cols

    if not has_metadata:
        log("⚠️ Tabela membersnextlevel sem coluna 'metadata'; não foi possível salvar botconversa_id")
        return

    set_parts = ["metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb"]
    bind_vals: List[Any] = [json.dumps({"botconversa_id": subscriber_id}, ensure_ascii=False)]
    if has_updated:
        set_parts.append("updated_at = NOW()")

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE membersnextlevel SET {', '.join(set_parts)} WHERE id=%s",
            (*bind_vals, member_id),
        )

def enqueue_validation_job(conn, member_id: int, email: str, nome: str, fonte: str = "sbcp"):
    cols = table_columns(conn, "validations_jobs")
    insert_cols = ["member_id", "email", "nome"]
    insert_vals_placeholders = ["%s", "%s", "%s"]
    bind_vals: List[Any] = [member_id, email, nome]

    if "fonte" in cols:
        insert_cols.append("fonte")
        insert_vals_placeholders.append("%s")
        bind_vals.append(fonte)
    if "status" in cols:
        insert_cols.append("status")
        insert_vals_placeholders.append("%s")
        bind_vals.append("PENDING")
    if "attempts" in cols:
        insert_cols.append("attempts")
        insert_vals_placeholders.append("%s")
        bind_vals.append(0)
    if "created_at" in cols:
        insert_cols.append("created_at")
        insert_vals_placeholders.append("NOW()")
    if "updated_at" in cols:
        insert_cols.append("updated_at")
        insert_vals_placeholders.append("NOW()")

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO validations_jobs ({', '.join(insert_cols)}) VALUES ({', '.join(insert_vals_placeholders)})",
            bind_vals,
        )
    log("📥 Job enfileirado", member_id=member_id, email=email, fonte=fonte, status="PENDING")

# -------------------- Payload parsing --------------------
def parse_fields() -> Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]:
    warns: Dict[str, Any] = {}
    body = coalesce_payload()

    email = first_present(
        body,
        "email", "e-mail", "e_mail", "mail", "contato_email", "contact_email",
    ) or ""

    full_name = first_present(
        body,
        "name", "nome", "full_name", "fullname", "full name", "nome completo", "first and last name",
    ) or ""

    phone = first_present(
        body,
        "phone", "telefone", "tel", "whatsapp", "celular", "mobile", "contato_telefone", "contact_phone",
    ) or ""

    phone_digits = normalize_phone_br(phone)
    if not full_name:
        full_name = "Visitante"
        warns["no_name"] = True
    if not email:
        if phone_digits:
            email = f"{phone_digits}@temp.nextlevelmedical.local"
            warns["email_fallback"] = "from_phone"
        else:
            email = f"lead-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}@temp.nextlevelmedical.local"
            warns["email_fallback"] = "timestamp"
    if not phone_digits and phone:
        warns["bad_phone_format"] = phone

    meta_extra = {"raw_payload": body}
    return email, full_name, phone_digits, meta_extra, warns

# -------------------- Routes --------------------
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
        email, full_name, phone_digits, extra_meta, warns = parse_fields()
        first_name, last_name = split_name(full_name)

        conn = db()

        # 1) Upsert membro
        member_id = upsert_member(conn, email=email, nome=full_name, phone_digits=phone_digits, extra_meta=extra_meta)

        # 2) BotConversa: cria/atualiza subscriber e salva id
        subscriber_id = None
        if phone_digits:
            subscriber_id = bc_create_or_update_subscriber(phone_digits=phone_digits, first_name=first_name, last_name=last_name)
            if subscriber_id:
                save_botconversa_id(conn, member_id, subscriber_id)
                log("🤝 BotConversa subscriber OK", subscriber_id=subscriber_id)
            else:
                log("⚠️ BotConversa subscriber não criado/atualizado (ver logs acima)")
        else:
            log("⚠️ Sem telefone normalizado; pulando BotConversa")

        # 3) Envia FLOW “em análise”
        if subscriber_id:
            ok = bc_send_flow(subscriber_id, BOTCONVERSA_FLOW_ANALISE)
            log("📨 BotConversa flow(analise)", ok=ok, flow_id=BOTCONVERSA_FLOW_ANALISE)

        # 4) Enfileira validação
        enqueue_validation_job(conn, member_id=member_id, email=email, nome=full_name, fonte="sbcp")

        resp = {
            "ok": True,
            "member_id": member_id,
            "subscriber_id": subscriber_id,
            "flow_id": BOTCONVERSA_FLOW_ANALISE if subscriber_id else None,
        }
        if warns:
            resp["warn"] = warns
        return jsonify(resp), 200

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log("💥 webhook_error", err=repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# -------------------- Main (local) --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=SERVICE_PORT)
