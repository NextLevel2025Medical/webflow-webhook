# -*- coding: utf-8 -*-
"""
Webhook (Webflow -> NextLevel) robusto e tolerante, com integração BotConversa.

Correções relevantes ao 400:
- Aceita JSON e form-urlencoded.
- Resolve campos por múltiplos sinônimos, case-insensitive.
- Se faltar email, gera fallback usando o telefone (evita 400).
- Se faltar nome, usa "Visitante" (first_name=Visitante, last_name="").
- Responde 200 mesmo com dados mínimos, e loga o motivo em "warn".

Fluxo:
1) Recebe payload do Webflow (JSON ou form).
2) Normaliza: email, nome (split first/last), phone (só dígitos; tenta adicionar 55).
3) Upsert em membersnextlevel (metadata acumula info útil).
4) BotConversa:
   4.1) Cria/atualiza subscriber -> salva subscriber_id no metadata.
   4.2) Envia FLOW "cadastro em análise" (default 7479821).
5) Enfileira job em validations_jobs com status PENDING.

Variáveis de ambiente (com defaults):
- DATABASE_URL
- BOTCONVERSA_API_KEY          (default: chave fornecida pelo cliente)
- BOTCONVERSA_BASE_URL         (default: https://backend.botconversa.com.br)
- BOTCONVERSA_FLOW_ANALISE     (default: 7479821)
- DEFAULT_COUNTRY_ISO          (default: BR) -> usado para DDI "55" no telefone, se faltar.
- SERVICE_PORT                 (default: 10000)
"""

import os
import re
import json
from datetime import datetime
from typing import Tuple, Optional, Dict, Any

import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

# -------------------- Config --------------------
DATABASE_URL = os.getenv("DATABASE_URL")

BOTCONVERSA_API_KEY = os.getenv(
    "BOTCONVERSA_API_KEY",
    "362e173a-ba27-4655-9191-b4fd735394da"  # ⚠️ ideal: mover para var de ambiente no Render
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
    """
    Mantém apenas dígitos e garante DDI 55 para BR se faltar.
    Aceita 10/11 dígitos (sem DDI) e 12/13 com DDI.
    """
    digits = only_digits(phone)
    if not digits:
        return ""
    # Se já vier com 55 e + número, mantemos
    if digits.startswith("55") and len(digits) >= 12:
        return digits
    # Se parecer número BR local (10/11 dígitos), prefixa 55
    if DEFAULT_COUNTRY_ISO == "BR" and 10 <= len(digits) <= 11:
        return f"55{digits}"
    return digits  # outros casos, devolve como está

def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "Visitante", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def first_present(d: Dict[str, Any], *keys) -> Optional[str]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return str(d[k]).strip()
    return None

def lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).strip().lower(): v for k, v in d.items()}

def coalesce_payload() -> Dict[str, Any]:
    """
    Une JSON body + form-urlencoded numa estrutura única (chaves lower-case).
    - request.get_json(silent=True)
    - request.form / request.values
    - se vier {"data": {...}}, usa "data" como base também
    """
    base: Dict[str, Any] = {}

    # JSON
    j = request.get_json(silent=True)
    if isinstance(j, dict):
        base.update(j)

    # FORM (prioriza se preencher algo que não veio no JSON)
    if request.form:
        for k in request.form:
            base.setdefault(k, request.form.get(k))

    # QUERYSTRING (último fallback — não deveria ser necessário)
    if request.args:
        for k in request.args:
            base.setdefault(k, request.args.get(k))

    # Se tiver camada "data"
    if isinstance(base.get("data"), dict):
        data = base["data"].copy()
        # se "form" vier aninhado dentro de "data"
        if isinstance(data.get("form"), dict):
            nested = {**data, **data["form"]}
            nested.pop("form", None)
            return lower_keys(nested)
        return lower_keys(data)

    # Se tiver camada "payload"
    if isinstance(base.get("payload"), dict):
        return lower_keys(base["payload"])

    return lower_keys(base)


# -------------------- BotConversa --------------------
def bc_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Content-Type": "application/json",
        "API-KEY": BOTCONVERSA_API_KEY,
    }

def bc_create_or_update_subscriber(phone_digits: str, first_name: str, last_name: str) -> Optional[int]:
    """
    POST /api/v1/webhook/subscriber/
    body: {"phone": "31986892292", "first_name": "...", "last_name": "..."}
    retorna int(id) ou None
    """
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    payload = {
        "phone": phone_digits,  # BotConversa aceita sem '+', só dígitos
        "first_name": first_name,
        "last_name": last_name,
    }
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
    """
    POST /api/v1/webhook/subscriber/{id}/send_flow/
    body: {"flow": <int>}
    """
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


# -------------------- DB ops --------------------
def upsert_member(conn, email: str, nome: str, phone_digits: str, extra_meta: Dict[str, Any]) -> int:
    """
    Insere/atualiza membro por email. Se não houver unique(email) na tabela, ajuste a query.
    """
    metadata_json = json.dumps({"phone": phone_digits, **extra_meta}, ensure_ascii=False)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO membersnextlevel (email, nome, metadata, created_at, updated_at)
            VALUES (%s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (email) DO UPDATE
               SET nome = EXCLUDED.nome,
                   metadata = COALESCE(membersnextlevel.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                   updated_at = NOW()
            RETURNING id
            """,
            (email, nome, metadata_json),
        )
        row = cur.fetchone()
        mid = int(row["id"])
        log("👤 UPSERT member", email=email, id=mid)
        return mid

def save_botconversa_id(conn, member_id: int, subscriber_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET metadata = COALESCE(metadata,'{}'::jsonb) || %s::jsonb,
                   updated_at = NOW()
             WHERE id = %s
            """,
            (json.dumps({"botconversa_id": subscriber_id}, ensure_ascii=False), member_id),
        )

def enqueue_validation_job(conn, member_id: int, email: str, nome: str, fonte: str = "sbcp"):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO validations_jobs (member_id, email, nome, fonte, status, attempts, created_at, updated_at)
            VALUES (%s, %s, %s, %s, 'PENDING', 0, NOW(), NOW())
            """,
            (member_id, email, nome, fonte),
        )
    log("📥 Job enfileirado", member_id=member_id, email=email, fonte=fonte, status="PENDING")


# -------------------- Payload parsing --------------------
def parse_fields() -> Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]:
    """
    Retorna: (email, full_name, phone_digits, meta_extra, debug_warns)
    - 'debug_warns' contém motivos de fallback (p/ log e retorno 200 informativo).
    """
    warns: Dict[str, Any] = {}
    body = coalesce_payload()

    # Chaves possíveis (minúsculas)
    email = first_present(
        body,
        "email", "e-mail", "e_mail", "mail",
        "contato_email", "contact_email",
    ) or ""

    full_name = first_present(
        body,
        "name", "nome", "full_name", "fullname", "full name",
        "nome completo", "first and last name",
    ) or ""

    phone = first_present(
        body,
        "phone", "telefone", "tel", "whatsapp", "celular", "mobile",
        "contato_telefone", "contact_phone",
    ) or ""

    # Fallbacks
    phone_digits = normalize_phone_br(phone)
    if not full_name:
        full_name = "Visitante"
        warns["no_name"] = True
    if not email:
        # gera e-mail temporário baseado no telefone; se também não houver telefone, usa timestamp
        if phone_digits:
            email = f"{phone_digits}@temp.nextlevelmedical.local"
            warns["email_fallback"] = "from_phone"
        else:
            email = f"lead-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}@temp.nextlevelmedical.local"
            warns["email_fallback"] = "timestamp"
    if not phone_digits and phone:
        # veio algo que não virou dígitos
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

        # 3) Envia FLOW “em análise”, se tiver subscriber_id
        flow_sent = None
        if subscriber_id:
            flow_sent = bc_send_flow(subscriber_id, BOTCONVERSA_FLOW_ANALISE)
            log("📨 BotConversa flow(analise)", ok=flow_sent, flow_id=BOTCONVERSA_FLOW_ANALISE)

        # 4) Enfileira validação
        enqueue_validation_job(conn, member_id=member_id, email=email, nome=full_name, fonte="sbcp")

        # 5) Retorno 200, com campo 'warn' quando houver fallback
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
        # Mesmo em erro interno, retorna 500 (p/ facilitar monitoramento)
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
