# -*- coding: utf-8 -*-
"""
Webhook (Webflow -> NextLevel) com integração BotConversa logo ao receber o cadastro.

Fluxo:
1) Recebe payload do Webflow.
2) Normaliza dados: email, nome (split em first/last), phone.
3) Upsert em membersnextlevel (metadata acumula info útil).
4) Chamada 1 (BotConversa): cria/atualiza subscriber -> salva subscriber_id no metadata.
5) Chamada 2 (BotConversa): envia FLOW de "cadastro em análise".
6) Enfileira job em validations_jobs com status PENDING (worker faz etapa 4 depois).

Variáveis de ambiente (com defaults seguros):
- DATABASE_URL
- BOTCONVERSA_API_KEY  (default: chave fornecida)
- BOTCONVERSA_BASE_URL (default: https://backend.botconversa.com.br)
- BOTCONVERSA_FLOW_ANALISE (default: 7479821)
"""

import os
import json
from datetime import datetime
from typing import Tuple, Optional, Dict, Any

import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

app = Flask(__name__)

# -------------------- Config --------------------
DATABASE_URL = os.getenv("DATABASE_URL")
BOTCONVERSA_API_KEY = os.getenv(
    "BOTCONVERSA_API_KEY",
    "362e173a-ba27-4655-9191-b4fd735394da"  # ⚠️ se possível, mova para variável de ambiente
)
BOTCONVERSA_BASE_URL = os.getenv("BOTCONVERSA_BASE_URL", "https://backend.botconversa.com.br")
BOTCONVERSA_FLOW_ANALISE = int(os.getenv("BOTCONVERSA_FLOW_ANALISE", "7479821"))

# -------------------- Utils --------------------
def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

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
    import re
    return re.sub(r"\D", "", s or "")

def split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def pick(d: dict, *keys) -> Optional[str]:
    for k in keys:
        if k in d and d[k]:
            return str(d[k]).strip()
    return None

# -------------------- BotConversa --------------------
def bc_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "Content-Type": "application/json",
        "API-KEY": BOTCONVERSA_API_KEY,
    }

def bc_create_or_update_subscriber(phone: str, first_name: str, last_name: str) -> Optional[int]:
    """
    POST /api/v1/webhook/subscriber/
    body: {"phone": "31986892292", "first_name": "...", "last_name": "..."}
    retorna int(id) ou None
    """
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/"
    payload = {"phone": phone, "first_name": first_name, "last_name": last_name}
    try:
        r = requests.post(url, headers=bc_headers(), json=payload, timeout=20)
        if not r.ok:
            log("BotConversa subscriber FAIL", status=r.status_code, body=r.text)
            return None
        data = r.json()
        sid = data.get("id")
        if isinstance(sid, int):
            return sid
        # algumas instalações retornam string
        try:
            return int(str(sid))
        except Exception:
            return None
    except Exception as e:
        log("BotConversa subscriber EXC", err=repr(e))
        return None

def bc_send_flow(subscriber_id: int, flow_id: int) -> bool:
    """
    POST /api/v1/webhook/subscriber/{id}/send_flow/
    body: {"flow": 7479821}
    """
    url = f"{BOTCONVERSA_BASE_URL.rstrip('/')}/api/v1/webhook/subscriber/{subscriber_id}/send_flow/"
    try:
        r = requests.post(url, headers=bc_headers(), json={"flow": flow_id}, timeout=20)
        ok = bool(r.ok)
        if not ok:
            log("BotConversa send_flow FAIL", status=r.status_code, body=r.text)
        return ok
    except Exception as e:
        log("BotConversa send_flow EXC", err=repr(e))
        return False

# -------------------- DB ops --------------------
def upsert_member(conn, email: str, nome: str, phone: str, extra_meta: Dict[str, Any]) -> int:
    """
    Insere/atualiza membro. Usa ON CONFLICT(email) se existir unique; senão, tenta select->insert.
    Retorna member_id.
    """
    metadata_json = json.dumps({"phone": phone, **extra_meta}, ensure_ascii=False)
    # tentativa com ON CONFLICT
    try:
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
            return int(row["id"])
    except Exception as e:
        log("upsert_member ON CONFLICT falhou, tentando fallback", err=repr(e))
        # fallback select/insert
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM membersnextlevel WHERE email=%s", (email,))
            row = cur.fetchone()
            if row:
                mid = int(row["id"])
                cur.execute(
                    """
                    UPDATE membersnextlevel
                       SET nome=%s,
                           metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                           updated_at=NOW()
                     WHERE id=%s
                    """,
                    (nome, metadata_json, mid),
                )
                return mid
            cur.execute(
                """
                INSERT INTO membersnextlevel (email, nome, metadata, created_at, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW(), NOW())
                RETURNING id
                """,
                (email, nome, metadata_json),
            )
            row = cur.fetchone()
            return int(row["id"])

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

# -------------------- Payload parsing --------------------
def parse_webflow_payload(body: dict) -> Tuple[str, str, str, Dict[str, Any]]:
    """
    Retorna: (email, nome_completo, phone, extra_meta)
    Aceita Webflow form submissions em formatos comuns.
    """
    # Webflow pode mandar como { "data": { ...campos... } } ou direto
    data = body.get("data") if isinstance(body, dict) and "data" in body else body
    data = data or {}

    email = pick(data, "email", "Email", "e-mail", "E-mail") or ""
    nome = pick(data, "name", "nome", "full_name", "Full Name", "Nome") or ""
    phone = pick(data, "phone", "telefone", "tel", "whatsapp", "Phone") or ""

    # se os campos vierem em outro subobjeto (ex.: form)
    if not email or not nome:
        form = data.get("form") if isinstance(data.get("form"), dict) else {}
        email = email or pick(form, "email", "Email")
        nome = nome or pick(form, "name", "nome")
        phone = phone or pick(form, "phone", "telefone", "whatsapp")

    return email or "", nome or "", phone or "", {"raw_payload": body}

# -------------------- Routes --------------------
@app.get("/health")
def health():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat()})

@app.post("/webflow-webhook")
def webflow_webhook():
    conn = None
    try:
        body = request.get_json(silent=True) or {}
        email, nome, phone, extra_meta = parse_webflow_payload(body)
        if not email or not nome:
            return jsonify({"ok": False, "error": "payload_invalido", "debug": {"email": email, "nome": nome}}), 400

        # normalizações
        phone_norm = only_digits(phone)
        first_name, last_name = split_name(nome)

        conn = db()

        # 1) upsert do membro (salvamos phone/metadados)
        member_id = upsert_member(conn, email=email, nome=nome, phone=phone_norm, extra_meta=extra_meta)
        log("member_upsert_ok", member_id=member_id, email=email)

        # 2) BotConversa: cria/atualiza subscriber (sempre)
        subscriber_id = bc_create_or_update_subscriber(phone=phone_norm, first_name=first_name, last_name=last_name)
        if subscriber_id:
            save_botconversa_id(conn, member_id, subscriber_id)
            log("botconversa_subscriber_ok", subscriber_id=subscriber_id)
            # 3) envia FLOW “em análise” imediatamente
            sent = bc_send_flow(subscriber_id, BOTCONVERSA_FLOW_ANALISE)
            log("botconversa_send_flow", ok=sent, flow_id=BOTCONVERSA_FLOW_ANALISE)
        else:
            log("botconversa_subscriber_none")

        # 4) enfileira validação para o worker (etapa 4 do seu processo)
        enqueue_validation_job(conn, member_id=member_id, email=email, nome=nome, fonte="sbcp")
        log("validation_job_enqueued", member_id=member_id)

        return jsonify({
            "ok": True,
            "member_id": member_id,
            "subscriber_id": subscriber_id,
            "flow_id": BOTCONVERSA_FLOW_ANALISE
        })

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log("webhook_error", err=repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    # Para rodar local: python webflow_payloads.py
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
