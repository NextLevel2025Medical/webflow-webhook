# worker_validation.py (sem resultados.json; import direto)
import os, json, time, signal, sys
from typing import Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import timezone, datetime

from consulta_medicos import buscar_sbcp  # <<< import direto

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("❌ DATABASE_URL não definido."); sys.exit(1)

engine = create_engine(DB_URL, pool_pre_ping=True)

POLL_SECONDS   = float(os.getenv("WORKER_POLL_SECONDS", "3"))
MAX_ATTEMPTS   = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))
VERBOSE_LOG    = os.getenv("WORKER_VERBOSE_LOG", "1") == "1"
FONTE_DEFAULT  = os.getenv("WORKER_FONTE_DEFAULT", "sbcp")
WORKER_STUB    = os.getenv("WORKER_STUB", "0") == "1"

_running = True
def _graceful(signum, frame):
    global _running; _running = False
signal.signal(signal.SIGTERM, _graceful); signal.signal(signal.SIGINT, _graceful)

def claim_job(conn):
    row = conn.execute(text("""
        SELECT id, member_id, email, nome, fonte, attempts
          FROM validation_jobs
         WHERE status='PENDING'
         ORDER BY created_at
         FOR UPDATE SKIP LOCKED
         LIMIT 1
    """)).fetchone()
    if not row: return None
    conn.execute(text("""
        UPDATE validation_jobs
           SET status='RUNNING', started_at=now(), attempts=attempts+1
         WHERE id=:id
    """), {"id": row.id})
    if VERBOSE_LOG:
        print(f"⚙️  Job {row.id} -> RUNNING (attempt {row.attempts+1}) [member_id={row.member_id} fonte={row.fonte}]", flush=True)
    return row

def map_status_to_pt(status: str) -> str:
    return {"ok":"aprovado","not_found":"recusado","error":"pendente"}.get(status,"pendente")

def finalize_job(conn, job_id: int, member_id: int, status: str, payload: Dict[str, Any]):
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    fonte = payload.get("fonte", FONTE_DEFAULT)

    novo_status = map_status_to_pt(status)
    conn.execute(text("""
        UPDATE membersnextlevel
           SET validacao_acesso=:novo_status,
               portal_validado=:fonte,
               validacao_at=now()
         WHERE id=:member_id
    """), {"novo_status": novo_status, "fonte": fonte, "member_id": member_id})

    conn.execute(text("""
        INSERT INTO validations_log (member_id, fonte, status, payload)
        VALUES (:member_id, :fonte, :status, CAST(:payload AS jsonb))
    """), {"member_id": member_id, "fonte": fonte, "status": status, "payload": payload_json})

    new_job_status = "DONE" if status in ("ok","not_found") else "FAILED"
    conn.execute(text("""
        UPDATE validation_jobs
           SET status=:new_status, finished_at=now(),
               last_error = CASE WHEN :new_status='FAILED' THEN :payload ELSE NULL END
         WHERE id=:id
    """), {"new_status": new_job_status, "payload": payload_json, "id": job_id})

    if VERBOSE_LOG:
        print(f"✅ Job {job_id} -> {new_job_status} (membro {member_id}: {novo_status}/{fonte})", flush=True)

def retry_or_fail(conn, job_row):
    if job_row.attempts >= MAX_ATTEMPTS:
        conn.execute(text("""
            UPDATE validation_jobs
               SET status='FAILED', finished_at=now(),
                   last_error=COALESCE(last_error,'tentativas excedidas')
             WHERE id=:id
        """), {"id": job_row.id})
        if VERBOSE_LOG: print(f"🧯 Job {job_row.id} -> FAILED definitivo.", flush=True)
        return False
    conn.execute(text("UPDATE validation_jobs SET status='PENDING' WHERE id=:id"), {"id": job_row.id})
    if VERBOSE_LOG: print(f"🔁 Job {job_row.id} re-enfileirado.", flush=True)
    return True

def run_validator(fonte: str, member_id: int, nome: str, email: str) -> Dict[str, Any]:
    fonte = (fonte or FONTE_DEFAULT).lower()
    if WORKER_STUB:
        return {"status":"ok","fonte":fonte,"raw":{"stub":True,"nome":nome,"email":email}}
    if fonte == "sbcp":
        return buscar_sbcp(nome, email)
    return {"status":"error","fonte":fonte,"reason":"fonte_desconhecida"}

def main_loop():
    print("🧵 Worker de validação iniciado.", flush=True)
    print(f"Config: POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS} FONTE_DEFAULT={FONTE_DEFAULT} STUB={WORKER_STUB}", flush=True)

    while _running:
        try:
            with engine.begin() as conn:
                job = claim_job(conn)
                if not job:
                    pass
                else:
                    result = run_validator(job.fonte, job.member_id, job.nome, job.email)
                    if result["status"] in ("ok","not_found"):
                        finalize_job(conn, job.id, job.member_id, result["status"], result)
                    else:
                        if retry_or_fail(conn, job):
                            if VERBOSE_LOG: print(f"⚠️  Job {job.id}: erro transitório; retry.", flush=True)
                        else:
                            finalize_job(conn, job.id, job.member_id, "error", result)
        except SQLAlchemyError as e:
            print(f"💥 Erro de banco: {e}", flush=True)
        except Exception as e:
            print(f"💥 Erro inesperado: {e}", flush=True)
        time.sleep(POLL_SECONDS)
    print("👋 Worker finalizado.", flush=True)

if __name__ == "__main__":
    main_loop()
