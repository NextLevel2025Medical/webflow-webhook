# webflow_payloads.py
# -------------------
# Webhook do Webflow/Cademi -> Render -> NeonDB
# - Arquiva TODO POST em webhook_members_audit (append-only)
# - Tenta inserir em membersnextlevel sem sobrescrever (ON CONFLICT DO NOTHING)
# - Marca audit como inserted | duplicate | invalid | error
# - Normaliza e-mail (strip + lower)
# - Enfileira validação em validation_jobs quando o membro está 'pending'
# - Parser resiliente para payloads:
#     A) { "event_id": "...", "event": { "usuario": { id, nome, email, celular, criado_em, ... } } }
#     B) { "event": { id, nome, email, celular, ... } }
#     C) { id, nome, email, celular, ... }  (fallback)

from flask import Flask, request, jsonify
import os
import json
import re
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
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def norm_email(s: str) -> str:
    """Normaliza e-mail para unicidade consistente."""
    return (s or "").strip().lower()


def is_valid_email(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    # validação simples para evitar falsos positivos
    return EMAIL_RE.match(s) is not None


def save_payload_locally(data: dict) -> None:
    """Guarda o payload num arquivo local (debug/troubleshooting)."""
    try:
        if not data:
            return
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


def parse_payload(data: dict):
    """
    Extrai campos de forma resiliente:
    - Suporta event.usuario (Cademi), event plano, ou raiz plana.
    Retorna dict com event_id, nome, email_raw, celular, created_at (string).
    """
    d = data or {}

    # Preferência: event_id na raiz (observado no log da Cademi)
    event_id = d.get("event_id") or d.get("id")

    evento = d.get("event") or {}
    usuario = {}
    if isinstance(evento, dict):
        # Cademi: { "event": { "usuario": {...} } }
        usuario = evento.get("usuario") or {}
        # Alguns provedores colocam direto: { "event": { id, nome, email } }
        if not usuario and any(k in evento for k in ("email", "nome", "celular", "id")):
            usuario = evento

    # fallback para raiz plana
    raiz = d if any(k in d for k in ("email", "nome", "celular")) else {}

    nome = (
        (usuario.get("nome") if isinstance(usuario, dict) else None)
        or evento.get("nome")
        or raiz.get("nome")
        or ""
    )
    email_raw = (
        (usuario.get("email") if isinstance(usuario, dict) else None)
        or evento.get("email")
        or raiz.get("email")
        or ""
    )
    celular = (
        (usuario.get("celular") if isinstance(usuario, dict) else None)
        or evento.get("celular")
        or raiz.get("celular")
        or ""
    )

    # created_at pode vir como "criado_em" no usuário (Cademi)
    created_at = (
        (usuario.get("criado_em") if isinstance(usuario, dict) else None)
        or d.get("created_at")
        or ""
    )

    # normaliza para string
    return {
        "event_id": str(event_id or ""),
        "nome": str(nome or ""),
        "email_raw": str(email_raw or ""),
        "celular": str(celular or ""),
        "created_at": str(created_at or ""),
    }


# -------------------
# Fila de validação
# -------------------
def enqueue_validation_job(conn, member_id: int):
    conn.execute(text("""
        INSERT INTO validation_jobs (member_id, email, nome, fonte)
        SELECT m.id, m.email, m.nome, 'sbcp'
          FROM membersnextlevel m
         WHERE m.id = :mid
           AND COALESCE(m.validacao_acesso, 'pendente') = 'pendente'
        ON CONFLICT (member_id, fonte) DO UPDATE
           SET status='PENDING', attempts=0, started_at=NULL, finished_at=NULL, last_error=NULL
         WHERE validation_jobs.status <> 'RUNNING'
    """), {"mid": member_id})


# -------------------
# DB persistence (append-only + insert-only)
# -------------------
def persist_db(data: dict):
    """
    1) Sempre arquiva a tentativa em webhook_members_audit (append-only).
    2) Se email for válido, tenta inserir no membersnextlevel SEM sobrescrever:
       ON CONFLICT(email) DO NOTHING.
    3) Atualiza o status no audit: inserted | duplicate | invalid | error.
    4) Enfileira validação em validation_jobs quando o membro estiver 'pending'.
    Retorna dict com {status, members_id, audit_id}.
    """
    if not engine:
        print("⚠️  DATABASE_URL não configurado; pulando persistência no Neon.", flush=True)
        return {"status": "no_db", "members_id": None, "audit_id": None}

    fields = parse_payload(data)
    event_id = fields["event_id"]
    nome = fields["nome"]
    email_raw = fields["email_raw"]
    email_norm = norm_email(email_raw)
    celular = fields["celular"]
    created_at = fields["created_at"]
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

        # 2) Se email inválido, apenas marca como invalid e não tenta inserir
        if not is_valid_email(email_norm):
            conn.execute(
                text("UPDATE webhook_members_audit SET status = 'invalid' WHERE id = :audit_id"),
                {"audit_id": audit_id},
            )
            return {"status": "invalid", "members_id": None, "audit_id": audit_id}

        try:
            # 3) Tenta gravação no members (insert-only)
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
                # INSERIU NOVO
                status = "inserted"
                members_id = row[0]

                # atualiza audit
                conn.execute(
                    text("UPDATE webhook_members_audit SET status = :status WHERE id = :audit_id"),
                    {"status": status, "audit_id": audit_id},
                )

                # ENFILEIRA VALIDAÇÃO (somente se ainda 'pending')
                enqueue_validation_job(conn, members_id)

                return {"status": status, "members_id": members_id, "audit_id": audit_id}

            else:
                # DUPLICADO: pegar o id existente para enfileirar (sem sobrescrever)
                existing = conn.execute(
                    text(
                        """
                        SELECT id, COALESCE(validacao_acesso, 'pending') AS st
                          FROM membersnextlevel
                         WHERE email = :email
                         LIMIT 1
                        """
                    ),
                    {"email": email_norm},
                ).fetchone()

                status = "duplicate"
                members_id = existing.id if existing else None

                # atualiza audit
                conn.execute(
                    text("UPDATE webhook_members_audit SET status = :status WHERE id = :audit_id"),
                    {"status": status, "audit_id": audit_id},
                )

                # se achou o membro e ainda está pendente, enfileira
                if existing and existing.st == "pending":
                    enqueue_validation_job(conn, members_id)

                return {"status": status, "members_id": members_id, "audit_id": audit_id}

        except SQLAlchemyError as e:
            # A transação atual será revertida automaticamente pelo context manager.
            # Abra uma NOVA transação para marcar o audit como 'error'.
            try:
                with engine.begin() as conn2:
                    conn2.execute(text("""
                        UPDATE webhook_members_audit
                           SET status = 'error', error_msg = :msg
                         WHERE id = :audit_id
                    """), {"msg": str(e), "audit_id": audit_id})
            except Exception as e2:
                print(f"⚠️ Falha ao marcar audit como error: {e2}", flush=True)
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

    try:
        f = parse_payload(data)
        print(
            f"    → event_id={f['event_id']} nome={f['nome']} email={f['email_raw']}",
            flush=True,
        )
    except Exception:
        pass

    save_payload_locally(data)

    try:
        result = persist_db(data)
        print(
            f"🗄️  Persistência DB → status={result['status']} "
            f"audit_id={result.get('audit_id')} members_id={result.get('members_id')}",
            flush=True,
        )
        # Retorne 200 sempre; status detalha o que ocorreu
        return jsonify({"ok": True, **result}), 200
    except Exception as e:
        print(f"⚠️  Falha ao gravar no DB: {e}", flush=True)
        # Evita retries agressivos: mantém 200 mas indica erro no corpo
        return jsonify({"ok": False, "error": str(e)}), 200


# -------------------
# Main
# -------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
