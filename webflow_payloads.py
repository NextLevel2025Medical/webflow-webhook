import os
import json
from datetime import datetime
from typing import Tuple, Optional

import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

# -----------------------------------------------------------------------------
# Flask app (precisa existir como variável de módulo para o Gunicorn encontrá-la)
# -----------------------------------------------------------------------------
app = Flask(__name__)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default

DATABASE_URL = (
    _env("DATABASE_URL")
    or f"postgresql://{_env('PGUSER','neon')}:{_env('PGPASSWORD','password')}@{_env('PGHOST','localhost')}:{_env('PGPORT','5432')}/{_env('PGDATABASE','neondb')}"
)

AUDIT_TABLE = _env("AUDIT_TABLE", "public.webhook_members_audit")
MEMBERS_TABLE = _env("MEMBERS_TABLE", "public.membersnextlevel")
JOBS_TABLE    = _env("JOBS_TABLE", "public.validations_jobs")  # nome com S

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def db():
    # autocommit False para controlar transações
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def jdump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    if kwargs:
        msg += " " + " ".join(f"{k}={v}" for k, v in kwargs.items())
    print(msg, flush=True)

# -----------------------------------------------------------------------------
# Parse helpers para o payload do Webflow
# -----------------------------------------------------------------------------
def parse_webflow_payload(body: dict) -> Tuple[str, str, str, str, dict]:
    """
    Retorna: (payload_id, nome, email, rqe, body_dict)
    Lê de:
      body['payload']['id']
      body['payload']['data']['nome'], ['email'], ['Rqe']
    """
    p = body.get("payload") or {}
    pid = str(p.get("id") or "").strip()

    data = p.get("data") or {}
    nome  = str(data.get("nome") or "").strip()
    email = str(data.get("email") or "").strip()
    rqe   = str(data.get("Rqe") or "").strip()

    return pid, nome, email, rqe, body

# -----------------------------------------------------------------------------
# SQL actions
# -----------------------------------------------------------------------------
def insert_audit(conn, payload_id: str, fonte: str, status: str, payload: dict):
    q = f"""
        INSERT INTO {AUDIT_TABLE} (payload_id, fonte, status, payload, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (payload_id) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(q, (payload_id, fonte, status, json.dumps(payload, ensure_ascii=False)))
    log("🗄️  Persistência DB (audit)", status=status, payload_id=payload_id)

def upsert_member(conn, nome: str, email: str, rqe: str, raw_payload: dict, celular: Optional[str]) -> int:
    """
    UPSERT no membersnextlevel, preenchendo:
      - nome, email
      - raw (merge jsonb)
      - metadata (merge jsonb) com celular e rqe
      - validacao_acesso = 'pending'
      - portal_validado  = 'cirurgiaplastica.org.br' (primeiro portal que vamos tentar)
    Retorna member_id (id).
    """
    # Monta jsons
    raw_json = {"source": "webflow", "received_at": datetime.utcnow().isoformat() + "Z"}
    # Merge do payload bruto (limitado) dentro de raw_json
    raw_json["payload_hint"] = {
        "has_data": bool(raw_payload.get("payload")),
        "form": (raw_payload.get("payload") or {}).get("name"),
    }

    # Metadata que sempre acumulamos
    metadata = {}
    if celular:
        metadata["celular"] = celular
    if rqe:
        metadata["rqe"] = rqe

    q = f"""
        INSERT INTO {MEMBERS_TABLE}
          (nome, email, raw, metadata, validacao_acesso, portal_validado, created_at)
        VALUES
          (%s,   %s,    COALESCE(%s::jsonb,'{{}}'::jsonb),
                 COALESCE(%s::jsonb,'{{}}'::jsonb),
                 'pending', 'cirurgiaplastica.org.br', NOW())
        ON CONFLICT (email) DO UPDATE
          SET nome = EXCLUDED.nome,
              raw  = COALESCE({MEMBERS_TABLE}.raw, '{{}}'::jsonb) || EXCLUDED.raw,
              metadata = COALESCE({MEMBERS_TABLE}.metadata, '{{}}'::jsonb) || EXCLUDED.metadata,
              validacao_acesso = 'pending',
              portal_validado  = 'cirurgiaplastica.org.br'
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(q, (
            nome,
            email,
            jdump(raw_json),
            jdump(metadata)
        ))
        mid = cur.fetchone()[0]
    log("👤 UPSERT member", email=email, id=mid)
    return mid

def enqueue_job(conn, member_id: int, nome: str, email: str):
    """
    Enfileira em validations_jobs (status PENDING).
    Requer UNIQUE(member_id, fonte) para DO NOTHING funcionar.
    """
    q = f"""
        INSERT INTO {JOBS_TABLE} (member_id, email, nome, fonte, status, attempts, created_at)
        VALUES (%s, %s, %s, 'cirurgiaplastica.org.br', 'PENDING', 0, NOW())
        ON CONFLICT (member_id, fonte) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(q, (member_id, email, nome))
    log("📥 Job enfileirado", member_id=member_id, email=email, fonte='cirurgiaplastica.org.br', status='PENDING')

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True, "ts": datetime.utcnow().isoformat() + "Z"})

@app.route("/webflow-webhook", methods=["POST"])
def webflow_webhook():
    source = "webflow"
    dry_run = request.args.get("dry_run") in ("1", "true", "yes")

    try:
        body = request.get_json(force=True, silent=False) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid-json"}), 400

    payload_id, nome, email, rqe, full = parse_webflow_payload(body)
    celular = (((full.get("payload") or {}).get("data") or {}).get("celular") or "").strip()

    log("📨 WEBFLOW",
        payload_id=payload_id or "(none)",
        form=(full.get("payload") or {}).get("name"),
        nome=nome, email=email, rqe=rqe)

    # Validação mínima de campos
    if not email or not nome:
        return jsonify({"ok": False, "error": "missing nome/email"}), 400

    conn = db()
    try:
        conn.autocommit = False

        # 1) AUDITORIA (sempre registramos)
        try:
            insert_audit(conn, payload_id or f"anon-{datetime.utcnow().timestamp()}", source, "received", full)
        except Exception as e_audit:
            # não bloqueia o fluxo principal
            log("⚠️ audit-fail", err=str(e_audit))

        # 2) UPSERT do membro na principal
        member_id = upsert_member(conn, nome=nome, email=email, rqe=rqe, raw_payload=full, celular=celular)

        # 3) Enfileira job (a menos que seja dry_run)
        if not dry_run:
            try:
                enqueue_job(conn, member_id=member_id, nome=nome, email=email)
            except Exception as e_job:
                # logamos erro na auditoria secundária (validations_log) ou apenas no console
                log("⚠️ enqueue-fail", err=str(e_job))
                # não damos rollback por isso — mantemos o membro salvo
        else:
            log("🧪 dry_run=1 → não criei job")

        conn.commit()
        return jsonify({
            "ok": True,
            "member_id": member_id,
            "queued": (not dry_run),
            "payload_id": payload_id
        })
    except Exception as e:
        conn.rollback()
        log("💥 ERRO HANDLER", err=repr(e))
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            conn.close()
        except:
            pass

# -----------------------------------------------------------------------------
# Dev server local (não usado no Render com gunicorn)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))

