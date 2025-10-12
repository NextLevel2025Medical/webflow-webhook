# webflow_payloads.py
import json
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, request

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL não definido no ambiente.", file=sys.stderr)
    sys.exit(1)

SCHEMA = "public"
TBL_AUDIT = f"{SCHEMA}.webhook_members_audit"
TBL_MEMBERS = f"{SCHEMA}.membersnextlevel"
TBL_JOBS = f"{SCHEMA}.validations_jobs"

FONTE = "webflow"          # origem do webhook
PORTAL_VALIDADO = "sbcp"   # portal alvo da validação

app = Flask(__name__)

# ------------------------------------------------------------------------------
# Helpers DB
# ------------------------------------------------------------------------------
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn

def dict_or_empty(x) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}

def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def audit_insert(conn, payload_id: str, status: str, payload_obj: Dict[str, Any]) -> None:
    """
    Registra na auditoria (idempotente por payload_id se desejar).
    schema esperado da tabela:
      (payload_id text PRIMARY KEY, fonte text, status text, payload jsonb, created_at timestamptz default now())
    """
    sql = f"""
        INSERT INTO {TBL_AUDIT} (payload_id, fonte, status, payload, created_at)
        VALUES (%(pid)s, %(fonte)s, %(status)s, %(payload)s, NOW())
        ON CONFLICT (payload_id) DO UPDATE
          SET status = EXCLUDED.status,
              payload = EXCLUDED.payload
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "pid": payload_id,
            "fonte": FONTE,
            "status": status,
            "payload": jdump(payload_obj),
        })

def upsert_member(conn, nome: str, email: str, celular: Optional[str], rqe: Optional[str],
                  raw_source: Dict[str, Any]) -> int:
    """
    UPSERT em membersnextlevel.
    Campos assumidos na tabela:
      - id bigserial PK
      - nome text
      - email text UNIQUE
      - raw jsonb NOT NULL
      - metadata jsonb
      - validacao_acesso text
      - portal_validado text
      - created_at timestamptz
    """
    # raw sempre não-nulo
    raw_json = {
        "source": FONTE,
        "payload_meta": {
            "received_at": datetime.utcnow().isoformat() + "Z"
        },
        **dict_or_empty(raw_source),
    }
    # metadata incorporando rqe + celular
    meta = {}
    if celular:
        meta["celular"] = celular
    if rqe:
        meta["rqe"] = rqe

    sql = f"""
        INSERT INTO {TBL_MEMBERS}
            (nome, email, raw, metadata, validacao_acesso, portal_validado, created_at)
        VALUES
            (%(nome)s, %(email)s, %(raw)s::jsonb, %(meta)s::jsonb, 'pending', %(portal)s, NOW())
        ON CONFLICT (email) DO UPDATE
          SET nome = EXCLUDED.nome,
              raw  = COALESCE({TBL_MEMBERS}.raw, '{{}}'::jsonb) || EXCLUDED.raw,
              metadata = COALESCE({TBL_MEMBERS}.metadata, '{{}}'::jsonb) || EXCLUDED.metadata,
              validacao_acesso = 'pending',
              portal_validado  = EXCLUDED.portal_validado
        RETURNING id
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "nome": nome,
            "email": email,
            "raw": jdump(raw_json),
            "meta": jdump(meta),
            "portal": PORTAL_VALIDADO,
        })
        row = cur.fetchone()
        return int(row[0])

def enqueue_validation_job(conn, member_id: int, nome: str, email: str) -> None:
    """
    Cria job idempotente por (member_id, fonte) em public.validations_jobs.
    Espera índice único: ux_validations_jobs_member_fonte (member_id, fonte)
    Colunas: (id, member_id, email, nome, fonte, status, attempts, created_at)
    """
    sql = f"""
        INSERT INTO {TBL_JOBS} (member_id, email, nome, fonte, status, attempts, created_at)
        VALUES (%(mid)s, %(email)s, %(nome)s, %(fonte)s, 'PENDING', 0, NOW())
        ON CONFLICT (member_id, fonte) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, {
            "mid": member_id,
            "email": email,
            "nome": nome,
            "fonte": PORTAL_VALIDADO,   # a fonte do JOB é o portal a validar
        })

# ------------------------------------------------------------------------------
# Parse do payload Webflow
# ------------------------------------------------------------------------------
def extract_webflow(payload: Dict[str, Any]) -> Tuple[str, str, str, Optional[str], Optional[str], Dict[str, Any]]:
    """
    Retorna (payload_id, nome, email, celular, rqe, raw_source)
    Espera entrada no formato:
    {
      "payload": {
        "id": "...",
        "name": "cirurgiao",
        "data": { "Rqe": "...", "nome": "...", "email": "...", "celular": "...", ... },
        ...
      },
      "triggerType": "form_submission"
    }
    """
    p = dict_or_empty(payload.get("payload"))
    payload_id = str(p.get("id") or f"wf-{datetime.utcnow().timestamp()}")

    data = dict_or_empty(p.get("data"))
    # chaves (Rqe | rqe)
    rqe = data.get("Rqe") or data.get("rqe") or data.get("RQE")
    if isinstance(rqe, (int, float)):
        rqe = str(rqe)
    elif isinstance(rqe, str):
        rqe = rqe.strip() or None
    else:
        rqe = None

    nome = (data.get("nome") or "").strip()
    email = (data.get("email") or "").strip().lower()
    celular = (data.get("celular") or "").strip() or None

    raw_source = {
        "webflow_name": p.get("name"),
        "webflow_siteId": p.get("siteId"),
        "webflow_pageUrl": p.get("pageUrl"),
        "webflow_submittedAt": p.get("submittedAt"),
        "formElementId": p.get("formElementId"),
    }

    return payload_id, nome, email, celular, rqe, raw_source

# ------------------------------------------------------------------------------
# Rota
# ------------------------------------------------------------------------------
@app.route("/webflow-webhook", methods=["POST"])
def webflow_webhook():
    client_ip = request.headers.get("x-forwarded-for") or request.remote_addr
    try:
        body = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"ok": False, "error": "corpo inválido (JSON)"}), 400

    dry_run = request.args.get("dry_run") in ("1", "true", "yes")
    payload_id, nome, email, celular, rqe, raw_source = extract_webflow(body or {})

    print(f"📨 WEBFLOW payload_id={payload_id} form={raw_source.get('webflow_name')} "
          f"nome={nome} email={email} rqe={rqe}")

    # validações mínimas
    if not nome or not email:
        # audita como inválido e encerra
        try:
            with get_conn() as conn:
                audit_insert(conn, payload_id, "invalid", body)
                conn.commit()
        except Exception as e:
            print(f"⚠️ falhou ao auditar inválido: {e}")
        return jsonify({"ok": False, "error": "nome/email ausentes"}), 200

    try:
        with get_conn() as conn:
            # 1) audit: received
            try:
                audit_insert(conn, payload_id, "received", body)
            except Exception as e_aud:
                # não bloqueia o fluxo principal
                print(f"⚠️ auditoria received falhou: {e_aud}")

            if dry_run:
                conn.commit()
                return jsonify({
                    "ok": True,
                    "dry_run": True,
                    "received": True,
                    "member_id": None
                }), 200

            # 2) upsert membro
            mid = upsert_member(
                conn,
                nome=nome,
                email=email,
                celular=celular,
                rqe=rqe,
                raw_source={"payload_id": payload_id, **raw_source},
            )

            # 3) cria job idempotente
            enqueue_validation_job(conn, member_id=mid, nome=nome, email=email)

            # 4) audit: inserted
            try:
                audit_insert(conn, payload_id, "inserted", body)
            except Exception as e_aud2:
                print(f"⚠️ auditoria inserted falhou: {e_aud2}")

            conn.commit()

        return jsonify({
            "ok": True,
            "member_id": mid,
            "queued": True,
            "fonte_job": PORTAL_VALIDADO
        }), 200

    except Exception as e:
        # Erro operacional: registrar na auditoria como "error"
        err_txt = f"{type(e).__name__}('{str(e)}')"
        print(f"💥 ERRO HANDLER: {err_txt}")
        try:
            with get_conn() as conn2:
                audit_insert(conn2, payload_id, "error", {
                    "body": body,
                    "exception": err_txt,
                    "traceback": traceback.format_exc(limit=3)
                })
                conn2.commit()
        except Exception as e2:
            print(f"⚠️ falhou ao logar erro em auditoria: {e2}")

        return jsonify({"ok": False, "error": str(e)}), 500

# ------------------------------------------------------------------------------
# healthcheck simples
# ------------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def root():
    return jsonify({"ok": True, "service": "webflow-webhook", "time": datetime.utcnow().isoformat() + "Z"}), 200


if __name__ == "__main__":
    # para rodar local:  python webflow_payloads.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
