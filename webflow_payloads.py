# webflow_payloads.py
# Endpoint Flask para receber SOMENTE o webhook do Webflow (Form Submission)

import os
import json
import datetime as dt
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text

app = Flask(__name__)

# --- Config DB ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não configurado")

if not DATABASE_URL.startswith(("postgresql://", "postgresql+psycopg2://")):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.environ.get("DB_POOL_SIZE", "5")),
    max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", "5")),
)

def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _norm(v):
    return (v or "").strip()

def _lower_keys(d: dict) -> dict:
    return {str(k).lower(): v for k, v in (d or {}).items()}

@app.get("/")
def root():
    return jsonify({
        "ok": True,
        "service": "webflow-webhook",
        "time": now_utc_iso()
    }), 404

@app.post("/webflow-webhook")
def webflow_webhook():
    body = request.get_json(silent=True) or {}
    trigger = body.get("triggerType")

    # Aceita apenas Webflow Form Submission
    if trigger != "form_submission":
        print(f"🚫 NOT_WEBFLOW_FORM: triggerType={trigger!r} ignorado")
        return jsonify({"ok": True, "skipped": "not_webflow_form"}), 200

    pf = body.get("payload") or {}
    data = pf.get("data") or {}
    payload_id = _norm(pf.get("id"))
    form_name = _norm(pf.get("name"))

    dlow = _lower_keys(data)
    nome    = _norm(dlow.get("nome") or dlow.get("nome_completo"))
    email   = _norm(dlow.get("email")).lower()
    celular = _norm(dlow.get("celular") or dlow.get("whatsapp"))
    rqe     = _norm(dlow.get("rqe") or dlow.get("rqe_crefito"))

    print(f"📨 WEBFLOW payload_id={payload_id} form={form_name} nome={nome} email={email} rqe={rqe}")

    # Campos mínimos
    if not nome or not email or not rqe:
        print("⚠️ MISSING_REQUIRED_FIELDS", {"nome": bool(nome), "email": bool(email), "rqe": bool(rqe)})
        return jsonify({
            "ok": True,
            "skipped": "missing_required_fields",
            "got": {"nome": bool(nome), "email": bool(email), "rqe": bool(rqe)}
        }), 200

    try:
        with ENGINE.begin() as conn:
            # 1) Auditoria + idempotência (payload_id)
            # OBS: Removido '::jsonb' — Postgres converte pelo tipo da coluna
            audit_res = conn.execute(text("""
                INSERT INTO webhook_members_audit (payload_id, fonte, status, payload, created_at)
                VALUES (:payload_id, 'webflow', 'received', :payload, NOW())
                ON CONFLICT (payload_id) DO NOTHING
            """), {"payload_id": payload_id, "payload": json.dumps(body)})

            if payload_id and audit_res.rowcount == 0:
                print(f"🔁 DEDUPE: payload_id={payload_id} já processado")
                return jsonify({"ok": True, "deduped": True}), 200

            # 2) Upsert do membro (doc = RQE), validacao pendente/sbcp
            row = conn.execute(text("""
                INSERT INTO membersnextlevel (nome, email, doc, metadata, validacao_acesso, portal_validado, created_at)
                VALUES (:nome, :email, :doc, jsonb_build_object('celular', :celular), 'pending', 'sbcp', NOW())
                ON CONFLICT (email) DO UPDATE
                  SET nome = EXCLUDED.nome,
                      doc  = EXCLUDED.doc,
                      metadata = COALESCE(membersnextlevel.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                      validacao_acesso = 'pending',
                      portal_validado  = 'sbcp'
                RETURNING id
            """), {"nome": nome, "email": email, "doc": rqe, "celular": celular}).first()

            if row is None:
                row = conn.execute(text("SELECT id FROM membersnextlevel WHERE email = :email"),
                                   {"email": email}).first()
            if row is None:
                raise RuntimeError("Não foi possível obter member_id após upsert")
            member_id = row[0]

            # 3) Enfileira job PENDING (idempotente por member_id)
            conn.execute(text("""
                INSERT INTO validations_jobs (member_id, email, nome, fonte, status, attempts, created_at)
                VALUES (:mid, :email, :nome, 'sbcp', 'PENDING', 0, NOW())
                ON CONFLICT (member_id) DO NOTHING
            """), {"mid": member_id, "email": email, "nome": nome})

        print(f"✅ PROCESSADO: member_id={member_id} → pending/sbcp")
        return jsonify({"ok": True, "member_id": member_id, "status": "pending"}), 200

    except Exception as e:
        print("💥 ERRO HANDLER:", repr(e))
        try:
            with ENGINE.begin() as conn:
                conn.execute(text("""
                    INSERT INTO validations_log (member_id, fonte, status, payload, created_at)
                    VALUES (NULL, 'webflow', 'error', :payload, NOW())
                """), {"payload": json.dumps({"error": str(e), "body": body})})
        except Exception as ee:
            print("⚠️ falhou ao logar erro:", repr(ee))
        return jsonify({"ok": False, "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), debug=True)

