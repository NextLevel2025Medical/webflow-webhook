# webflow_payloads.py
import os
import json
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

# -----------------------------
# Config
# -----------------------------
def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default

DATABASE_URL = _env("DATABASE_URL") or (
    f"postgresql://{_env('PGUSER','neon')}:"
    f"{_env('PGPASSWORD','password')}@{_env('PGHOST','localhost')}:"
    f"{_env('PGPORT','5432')}/{_env('PGDATABASE','neondb')}"
)

APP_PORT = int(_env("PORT", "10000"))

app = Flask(__name__)

# -----------------------------
# DB helpers
# -----------------------------
def db_conn():
    conn = psycopg2.connect(DATABASE_URL)
    # permite retornar dicts
    conn.autocommit = False
    return conn

def jprint(prefix, **data):
    # log padronizado
    payload = " ".join([f'{k}={v}' for k, v in data.items()])
    print(f"{prefix} {payload}")

# -----------------------------
# Util
# -----------------------------
def pick_webflow_fields(body: dict):
    """
    Extrai campos do payload do Webflow Forms:
      {
        "payload": {
          "id": "...",
          "data": {
            "Rqe": "12345",
            "nome": "Fulano",
            "email": "fulano@ex.com",
            "celular": "119...",
            "Você é cirurgião plástico": "Sim"
          },
          ...
        },
        "triggerType": "form_submission"
      }
    """
    p = (body or {}).get("payload", {}) or {}
    pdata = p.get("data", {}) or {}

    payload_id = p.get("id") or p.get("_id") or ""
    nome = pdata.get("nome") or ""
    email = (pdata.get("email") or "").strip().lower()
    celular = pdata.get("celular") or ""
    rqe = pdata.get("Rqe") or pdata.get("rqe") or ""
    form_name = p.get("name") or ""

    return {
        "payload_id": payload_id,
        "form_name": form_name,
        "nome": nome,
        "email": email,
        "celular": celular,
        "rqe": rqe,
        "raw_payload": p  # apenas a parte "payload" do Webflow
    }

# -----------------------------
# Rotas
# -----------------------------
@app.route("/", methods=["GET"])
def root():
    return jsonify({"ok": False, "message": "use POST /webflow-webhook"}), 404

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200

@app.route("/webflow-webhook", methods=["POST"])
def webflow_webhook():
    dry_run = request.args.get("dry_run", "0") in ("1", "true", "True")
    try:
        body = request.get_json(force=True, silent=False)
    except Exception as e:
        return jsonify({"ok": False, "error": f"invalid json: {e}"}), 400

    fields = pick_webflow_fields(body)
    payload_id = fields["payload_id"]
    nome = fields["nome"]
    email = fields["email"]
    celular = fields["celular"]
    rqe = fields["rqe"]
    form_name = fields["form_name"]

    jprint("📨 WEBFLOW",
           payload_id=payload_id,
           form=form_name,
           nome=nome,
           email=email,
           rqe=rqe)

    # validação mínima
    if not email or not payload_id:
        return jsonify({"ok": False, "error": "missing email or payload_id"}), 400

    # Conecta no DB
    conn = db_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        # 1) Auditoria sempre primeiro
        if not dry_run:
            cur.execute("""
                INSERT INTO public.webhook_members_audit (payload_id, fonte, status, payload, created_at)
                VALUES (%(payload_id)s, 'webflow', 'received', %(payload)s, NOW())
                ON CONFLICT (payload_id) DO NOTHING
            """, {
                "payload_id": payload_id,
                "payload": json.dumps(body, ensure_ascii=False)
            })

        # 2) Upsert do member
        #    - raw: guarda marcação de origem
        #    - metadata: merge com rqe e celular
        raw_json = {"source": "webflow"}
        meta_json = {"celular": celular}
        if rqe:
            meta_json["rqe"] = rqe

        # retorna (id, email, nome)
        cur.execute("""
            WITH upsert AS (
              INSERT INTO public.membersnextlevel
                (nome, email, raw, metadata, validacao_acesso, portal_validado, created_at)
              VALUES
                (%(nome)s, %(email)s,
                 %(raw)s::jsonb,
                 %(meta)s::jsonb,
                 'pending', 'sbcp', NOW())
              ON CONFLICT (email) DO UPDATE
                 SET nome = EXCLUDED.nome,
                     raw  = COALESCE(public.membersnextlevel.raw, '{}'::jsonb) || EXCLUDED.raw,
                     metadata = COALESCE(public.membersnextlevel.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                     validacao_acesso = 'pending',
                     portal_validado  = 'sbcp'
              RETURNING id, email, nome
            )
            SELECT id, email, nome FROM upsert
        """, {
            "nome": nome,
            "email": email,
            "raw": json.dumps(raw_json, ensure_ascii=False),
            "meta": json.dumps(meta_json, ensure_ascii=False),
        })
        row = cur.fetchone()
        if not row:
            raise RuntimeError("member upsert returned no row")
        member_id = int(row["id"])

        # 3) Enfileira validação (a menos que dry_run)
        queued = False
        if not dry_run:
            # abre (ou reabre) job p/ (member_id,'sbcp')
            cur.execute("""
                INSERT INTO public.validations_jobs (member_id, email, nome, fonte, status, attempts, created_at)
                VALUES (%(mid)s, %(email)s, %(nome)s, 'sbcp', 'PENDING', 0, NOW())
                ON CONFLICT (member_id, fonte) DO UPDATE
                  SET status = CASE
                                  WHEN public.validations_jobs.status IN ('FAILED','SUCCEEDED') THEN 'PENDING'
                                  ELSE public.validations_jobs.status
                               END,
                      attempts = CASE
                                   WHEN public.validations_jobs.status IN ('FAILED','SUCCEEDED') THEN 0
                                   ELSE public.validations_jobs.attempts
                                 END
                RETURNING id
            """, {"mid": member_id, "email": email, "nome": nome})
            job = cur.fetchone()
            queued = bool(job and job["id"])

        conn.commit()

        return jsonify({
            "ok": True,
            "dry_run": dry_run,
            "member_id": member_id,
            "fonte_job": "sbcp",
            "queued": queued
        }), 200

    except Exception as e:
        conn.rollback()
        jprint("💥 ERRO HANDLER:", err=str(e))
        # tenta logar erro em validations_log (se existir)
        try:
            cur.execute("""
                INSERT INTO public.validations_log (member_id, fonte, status, payload, created_at)
                VALUES (NULL, 'webflow', 'error', %(payload)s, NOW())
            """, {"payload": json.dumps({"body": body, "error": str(e)}, ensure_ascii=False)})
            conn.commit()
        except Exception as e2:
            conn.rollback()
            jprint("⚠️ falhou ao logar erro:", err=str(e2))

        return jsonify({"ok": False, "error": str(e)}), 500

    finally:
        try:
            cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass

# -----------------------------
# Local run
# -----------------------------
if __name__ == "__main__":
    print(f"Running on 0.0.0.0:{APP_PORT}")
    app.run(host="0.0.0.0", port=APP_PORT)
