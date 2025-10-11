# webflow_payloads.py
# -------------------
# Webhook do Webflow -> Render -> NeonDB
# - Arquiva TODO POST em webhook_members_audit (append-only)
# - Tenta inserir em membersnextlevel sem sobrescrever (ON CONFLICT DO NOTHING)
# - Marca audit como inserted | duplicate | error
# - Normaliza e-mail (strip + lower)

from flask import Flask, request, jsonify
import os
import json
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -------------------
# Config
# -------------------
app = Flask(__name__)
OUTPUT_FILE = os.getenv("WEBFLOW_OUTPUT_FILE", "webflow_payloads.json")

DB_URL = os.getenv("DATABASE_URL")  # ex: postgres://user:pass@host/db
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None


# -------------------
# Utils
# -------------------
def norm_email(s: str) -> str:
    """Normaliza e-mail para unicidade consistente."""
    return (s or "").strip().lower()


def save_payload_locally(data: dict) -> None:
    """Guarda o payload num arquivo local (debug/troubleshooting)."""
    try:
        if not data:
            return
        # salva como lista de eventos
        payload = []
        if os.path.exists(OUTPUT_FILE):
            try:
                with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                    if not isinstance(payload, list):
                        payload = []
            except Exception:
                payload = []
        payload.append(
            {
                "received_at": datetime.utcnow().isoformat() + "Z",
                "data": data,
            }
        )
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  Falha ao salvar payload local: {e}", flush=True)


# -------------------
# DB persistence (append-only + insert-only)
# -------------------
def persist_db(data: dict):
    """
    1) Sempre arquiva a tentativa em webhook_members_audit (append-only).
    2) Tenta inserir no membersnextlevel SEM sobrescrever: ON CONFLICT(email) DO NOTHING.
    3) Atualiza o status no audit: inserted | duplicate | error.
    Retorna dict com {status, members_id, audit_id}.
    """
    if not engine:
        print("⚠️  DATABASE_URL não configurado; pulando persistência no Neon.", flush=True)
        return {"status": "no_db", "members_id": None, "audit_id": None}

    evento = (data or {}).get("event", {}) or {}
    event_id = str(evento.get("id") or "")
    nome = str(evento.get("nome") or "")
    email_raw = str(evento.get("email") or "")
    email_norm = norm_email(email_raw)
    celular = str(evento.get("celular") or "")
    created_at = str((data or {}).get("created_at") or "")
    raw_json = json.dumps(data, ensure_ascii=False)

    with engine.begin() as conn:
        # 1) Insere audit como 'pending'
        audit_id = conn.execute(
            text(
                """
                INSERT INTO webhook_members_audit
                    (event_id, source, email_raw, email_norm, status, payload_raw)
                VALUES
                    (:event_id, :source, :email_raw, :email_norm, 'pending', :payload_raw)
                RETURNING id
                """
            ),
            {
                "event_id": event_id,
                "source": "webflow",
                "email_raw": email_raw,
                "email_norm": email_norm,
                "payload_raw": raw_json,
            },
        ).scalar_one()

        try:
            # 2) Tenta gravação no members (insert-only)
            row = conn.execute(
                text(
                    """
                    INSERT INTO membersnextlevel (event_id, nome, email, celular, created_at, raw)
                    VALUES (:event_id, :nome, :email, :celular, :created_at, :raw)
                    ON CONFLICT (email) DO NOTHING
                    RETURNING id
                    """
                ),
                {
                    "event_id": event_id,
                    "nome": nome,
                    "email": email_norm,
                    "celular": celular,
                    "created_at": created_at,
                    "raw": raw_json,
                },
            ).fetchone()

            if row:
                status = "inserted"
                members_id = row[0]
            else:
                status = "duplicate"
                members_id = None

            # 3) Atualiza o audit com o status final
            conn.execute(
                text(
                    """
                    UPDATE webhook_members_audit
                       SET status = :status
                     WHERE id = :audit_id
                    """
                ),
                {"status": status, "audit_id": audit_id},
            )

            return {"status": status, "members_id": members_id, "audit_id": audit_id}

        except SQLAlchemyError as e:
            # Registra erro no audit e reergue
            conn.execute(
                text(
                    """
                    UPDATE webhook_members_audit
                       SET status = 'error', error_msg = :msg
                     WHERE id = :audit_id
                    """
                ),
                {"msg": str(e), "audit_id": audit_id},
            )
            raise


# -------------------
# Routes
# -------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}), 200


@app.route("/webflow-webhook", methods=["POST"])
def webflow_webhook():
    data = request.get_json(silent=True) or {}

    print("🔔 Webhook recebido", flush=True)
    # debug rápido (mostra só campos principais para não lotar log)
    try:
        evento = (data or {}).get("event", {}) or {}
        print(
            f"    → event_id={evento.get('id')} nome={evento.get('nome')} email={evento.get('email')}",
            flush=True,
        )
    except Exception:
        pass

    # Arquiva local (útil para troubleshooting)
    save_payload_locally(data)

    # Persistência no Neon
    try:
        result = persist_db(data)
        print(
            f"🗄️  Persistência DB → status={result['status']} "
            f"audit_id={result.get('audit_id')} members_id={result.get('members_id')}",
            flush=True,
        )
        # Retorne 200 sempre que o request foi processado (mesmo duplicate)
        return jsonify({"ok": True, **result}), 200
    except Exception as e:
        print(f"⚠️  Falha ao gravar no DB: {e}", flush=True)
        # Evite retry infinito do provedor: responda 200 mas indique erro no payload
        return jsonify({"ok": False, "error": str(e)}), 200


# -------------------
# Main
# -------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
