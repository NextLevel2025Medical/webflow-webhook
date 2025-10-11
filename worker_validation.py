# worker_validation.py
# Loop que consome validation_jobs e valida no(s) portal(is).
import os
import time
import json
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

# ====== Config por ENV ======
DB_URL        = os.getenv("DATABASE_URL")
POLL_SECONDS  = float(os.getenv("POLL_SECONDS", "3.0"))
MAX_ATTEMPTS  = int(os.getenv("MAX_ATTEMPTS", "3"))
FONTE_DEFAULT = os.getenv("FONTE_DEFAULT", "sbcp").lower().strip()
STUB          = os.getenv("STUB", "false").lower() in ("1", "true", "yes", "on")

# ====== Importa o conector do(s) portal(is) ======
try:
    from consulta_medicos import buscar_sbcp
except Exception as e:
    print("❌ Falha importando consulta_medicos:", e)
    buscar_sbcp = None  # será checado adiante

# ====== Helpers ======
def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def db_connect():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn

def dict_cur(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ====== SQL (ajuste nomes se necessário) ======
SQL_RESERVA = """
WITH nxt AS (
  SELECT id
  FROM validation_jobs
  WHERE status IN ('PENDING', 'RETRY')
    AND attempts < %(max_attempts)s
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE validation_jobs j
   SET status      = 'RUNNING',
       started_at  = NOW(),
       attempts    = attempts + 1
  FROM nxt
 WHERE j.id = nxt.id
RETURNING j.id, j.member_id, j.email, j.nome, j.fonte, j.status, j.attempts;
"""

SQL_LOG_INS = """
INSERT INTO validations_log (member_id, fonte, status, payload)
VALUES (%(member_id)s, %(fonte)s, %(status)s, %(payload)s::jsonb)
"""

SQL_JOB_DONE = """
UPDATE validation_jobs
   SET status = %(status)s,
       finished_at = NOW(),
       last_error = %(err)s
 WHERE id = %(job_id)s
"""

SQL_APROVA_MEMBRO = """
UPDATE membersnextlevel
   SET validacao_acesso = 'aprovado',
       portal_validado  = %(fonte)s
 WHERE id = %(member_id)s
"""

# ====== Execução de uma validação ======
def run_validation(job, conn):
    job_id    = job["id"]
    member_id = job["member_id"]
    email     = (job.get("email") or "").strip()
    nome      = (job.get("nome") or "").strip()
    fonte     = (job.get("fonte") or FONTE_DEFAULT).lower()

    print(f"⚙️  Job {job_id} -> RUNNING (attempt {job['attempts']}) [member_id={member_id} fonte={fonte}]")

    # 1) Executa consulta ao portal (ou STUB)
    if STUB:
        resultado = {
            "status": "ok",
            "fonte": fonte,
            "raw": {
                "nome_busca": nome,
                "email": email,
                "qtd": 1,
                "resultados": [],
                "timing_ms": 5,
                "debug": {"steps": ["stub: ok simulado"]}
            }
        }
    else:
        if fonte == "sbcp":
            if buscar_sbcp is None:
                raise RuntimeError("consulta_medicos.buscar_sbcp indisponível")
            resultado = buscar_sbcp(nome, email)
        else:
            resultado = {
                "status": "error",
                "fonte": fonte,
                "raw": {"nome_busca": nome, "email": email, "qtd": 0, "resultados": []},
                "reason": f"fonte_nao_suportada:{fonte}"
            }

    status_ext = resultado.get("status", "error")
    raw        = resultado.get("raw", {})
    reason     = resultado.get("reason")
    steps      = (raw.get("debug") or {}).get("steps", [])

    # 2) Loga passos principais no console para diagnóstico
    if steps:
        print("🔎 DEBUG STEPS (finais):")
        for line in steps[-12:]:
            print("   •", line)

    # 3) Registra LOG no DB
    payload_json = json.dumps(raw, ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute(SQL_LOG_INS, {
            "member_id": member_id,
            "fonte": fonte,
            "status": status_ext,
            "payload": payload_json,
        })

    # 4) Decide ação sobre o membro e o job
    if status_ext == "ok":
        # Aprova o membro
        try:
            with conn.cursor() as cur:
                cur.execute(SQL_APROVA_MEMBRO, {
                    "member_id": member_id,
                    "fonte": fonte
                })
            with conn.cursor() as cur:
                cur.execute(SQL_JOB_DONE, {
                    "status": "SUCCEEDED",
                    "err": None,
                    "job_id": job_id
                })
            print(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado/{fonte})")
        except Exception as e:
            # Se falhar a atualização do membro, grava erro no job
            with conn.cursor() as cur:
                cur.execute(SQL_JOB_DONE, {
                    "status": "FAILED",
                    "err": f"update_member_fail: {e}",
                    "job_id": job_id
                })
            print(f"💥 Erro de banco ao aprovar membro {member_id}: {e}")

    elif status_ext in ("not_found", "error"):
        # Não mexe no membersnextlevel; finaliza como FAILED
        err_msg = reason or status_ext
        with conn.cursor() as cur:
            cur.execute(SQL_JOB_DONE, {
                "status": "FAILED",
                "err": err_msg,
                "job_id": job_id
            })
        print(f"🧯 Job {job_id} -> FAILED (membro {member_id}: pendente/{fonte})")

    else:
        # fallback
        with conn.cursor() as cur:
            cur.execute(SQL_JOB_DONE, {
                "status": "FAILED",
                "err": f"status_desconhecido:{status_ext}",
                "job_id": job_id
            })
        print(f"🧯 Job {job_id} -> FAILED (status_externo_desconhecido={status_ext})")

# ====== Loop principal ======
def main():
    print("🧵 Worker de validação iniciado.")
    print(f"Config: POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS} FONTE_DEFAULT={FONTE_DEFAULT} STUB={STUB}")

    conn = db_connect()
    try:
        while True:
            job = None
            try:
                with dict_cur(conn) as cur:
                    cur.execute(SQL_RESERVA, {"max_attempts": MAX_ATTEMPTS})
                    job = cur.fetchone()

                if not job:
                    time.sleep(POLL_SECONDS)
                    continue

                # Executa o job reservado
                try:
                    run_validation(job, conn)
                except Exception as e:
                    # erro inesperado durante a execução: marca FAILED
                    with conn.cursor() as cur:
                        cur.execute(SQL_JOB_DONE, {
                            "status": "FAILED",
                            "err": f"exception:{e}",
                            "job_id": job["id"]
                        })
                    print(f"💥 Job {job['id']} falhou com exceção: {e}")

            except Exception as loop_err:
                # erro ao reservar job ou erro externo de conexão
                print("⚠️  Erro no loop:", loop_err)
                time.sleep(max(POLL_SECONDS, 2.0))
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
