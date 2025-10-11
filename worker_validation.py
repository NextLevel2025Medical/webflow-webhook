# worker_validation.py
# --------------------
# Consome a fila validation_jobs no Postgres (Neon) e valida cadastros
# - Pega 1 job PENDING com FOR UPDATE SKIP LOCKED
# - Marca RUNNING, executa a validação (fonte 'sbcp' via consulta_medicos.py)
# - Atualiza membersnextlevel.validacao_acesso/portal_validado/validacao_at
# - Registra rastro em validations_log
# - DONE (ok|not_found) ou FAILED (com retry até MAX_ATTEMPTS)
#
# Requisitos:
# - Variável de ambiente DATABASE_URL
# - Arquivo consulta_medicos.py acessível (mesmo diretório)
#
# Observações:
# - Mantém o webhook rápido (validação não roda no request)
# - Idempotente via SKIP LOCKED; suporta múltiplos workers em paralelo

import os, json, time, shlex, subprocess, signal, sys
from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ========= Config =========
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    print("❌ DATABASE_URL não definido. Configure a variável de ambiente.", flush=True)
    sys.exit(1)

engine = create_engine(DB_URL, pool_pre_ping=True)

# Tuning
POLL_SECONDS   = float(os.getenv("WORKER_POLL_SECONDS", "3"))   # intervalo entre buscas
MAX_ATTEMPTS   = int(os.getenv("WORKER_MAX_ATTEMPTS", "3"))     # tentativas por job
JOB_TIMEOUT_S  = int(os.getenv("WORKER_JOB_TIMEOUT_S", "120"))  # timeout da execução do coletor
VERBOSE_LOG    = os.getenv("WORKER_VERBOSE_LOG", "1") == "1"    # logs detalhados
FONTE_DEFAULT  = os.getenv("WORKER_FONTE_DEFAULT", "sbcp")      # fonte padrão

_running = True

def _graceful_shutdown(signum, frame):
    global _running
    print(f"🛑 Sinal recebido ({signum}). Encerrando loop após o job atual...", flush=True)
    _running = False

# Sinais do Render / Linux
signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT,  _graceful_shutdown)


# ========= Funções de DB =========
def claim_job(conn):
    """
    Seleciona 1 job PENDING e marca como RUNNING (com lock).
    """
    row = conn.execute(text("""
        SELECT id, member_id, email, nome, fonte, attempts
        FROM validation_jobs
        WHERE status = 'PENDING'
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    """)).fetchone()

    if not row:
        return None

    conn.execute(text("""
        UPDATE validation_jobs
           SET status='RUNNING', started_at=now(), attempts=attempts+1
         WHERE id=:id
    """), {"id": row.id})

    if VERBOSE_LOG:
        print(f"⚙️  Job {row.id} -> RUNNING (attempt {row.attempts + 1}) "
              f"[member_id={row.member_id} fonte={row.fonte}]", flush=True)
    return row


def finalize_job(conn, job_id: int, member_id: int, status: str, payload: Dict[str, Any]):
    """
    Finaliza job como DONE/FAILED e grava log + atualiza membro.
    status: 'ok' | 'not_found' | 'error'
    """
    payload_json = json.dumps(payload or {}, ensure_ascii=False)

    # Atualiza membro conforme resultado
    if status == "ok":
        conn.execute(text("""
            UPDATE membersnextlevel
               SET validacao_acesso='ok',
                   portal_validado=:fonte,
                   validacao_at=now()
             WHERE id=:member_id
        """), {"member_id": member_id, "fonte": payload.get("fonte", FONTE_DEFAULT)})

    elif status == "not_found":
        conn.execute(text("""
            UPDATE membersnextlevel
               SET validacao_acesso='not_found',
                   portal_validado=:fonte,
                   validacao_at=now()
             WHERE id=:member_id
        """), {"member_id": member_id, "fonte": payload.get("fonte", FONTE_DEFAULT)})

    else:
        # erro transitório: mantém 'pending' para futuros reprocessos manuais/novos jobs
        # (dependendo da sua política, você pode marcar explicitamente 'error' aqui)
        conn.execute(text("""
            UPDATE membersnextlevel
               SET validacao_acesso='pending',
                   portal_validado=:fonte
             WHERE id=:member_id
        """), {"member_id": member_id, "fonte": payload.get("fonte", FONTE_DEFAULT)})

    # Log detalhado
    conn.execute(text("""
        INSERT INTO validations_log (member_id, fonte, status, payload)
        VALUES (:member_id, :fonte, :status, :payload::jsonb)
    """), {
        "member_id": member_id,
        "fonte": payload.get("fonte", FONTE_DEFAULT),
        "status": status,
        "payload": payload_json
    })

    new_status = "DONE" if status in ("ok", "not_found") else "FAILED"
    conn.execute(text("""
        UPDATE validation_jobs
           SET status=:new_status,
               finished_at=now(),
               last_error = CASE WHEN :new_status='FAILED' THEN :err ELSE NULL END
         WHERE id=:id
    """), {"new_status": new_status, "err": payload_json if new_status=="FAILED" else None, "id": job_id})

    if VERBOSE_LOG:
        print(f"✅ Job {job_id} -> {new_status.upper()} ({status})", flush=True)


def retry_or_fail(conn, job_row):
    """
    Se não atingiu MAX_ATTEMPTS, volta job para PENDING.
    Senão, marca FAILED definitivo.
    """
    if job_row.attempts >= MAX_ATTEMPTS:
        conn.execute(text("""
            UPDATE validation_jobs
               SET status='FAILED', finished_at=now(),
                   last_error = COALESCE(last_error, 'tentativas excedidas')
             WHERE id=:id
        """), {"id": job_row.id})
        if VERBOSE_LOG:
            print(f"🧯 Job {job_row.id} -> FAILED definitivo (tentativas excedidas).", flush=True)
        return False

    conn.execute(text("UPDATE validation_jobs SET status='PENDING' WHERE id=:id"), {"id": job_row.id})
    if VERBOSE_LOG:
        print(f"🔁 Job {job_row.id} re-enfileirado (nova tentativa futura).", flush=True)
    return True


# ========= Execução da validação =========
def run_validator(fonte: str, member_id: int, nome: str, email: str) -> Dict[str, Any]:
    """
    Roteia para a fonte de validação.
    Para 'sbcp', chama o seu consulta_medicos.py (Selenium).
    """
    fonte = (fonte or FONTE_DEFAULT).lower()

    if fonte == "sbcp":
        return _validate_sbcp(member_id, nome, email)
    else:
        # outras fontes no futuro: 'cfm', 'crefito', etc.
        return {"status": "error", "fonte": fonte, "reason": "fonte_desconhecida"}


def _validate_sbcp(member_id: int, nome: str, email: str) -> Dict[str, Any]:
    """
    Invoca seu coletor Selenium em modo "1 registro" e lê o resultados.json.
    Espera-se que consulta_medicos.py grave uma linha com {"id": member_id, ...}.
    """
    cmd = f'python consulta_medicos.py {member_id} "{nome}" "" "{email}" "{datetime.utcnow().isoformat()}Z"'
    if VERBOSE_LOG:
        print(f"🧪 Executando SBCP: {cmd}", flush=True)

    try:
        out = subprocess.run(
            shlex.split(cmd),
            capture_output=True,
            text=True,
            timeout=JOB_TIMEOUT_S
        )
        # Procura o resultado do member_id em resultados.json
        try:
            with open("resultados.json", "r", encoding="utf-8") as f:
                last_match = None
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if str(obj.get("id")) == str(member_id):
                        last_match = obj
                if last_match:
                    status_txt = (last_match.get("status") or "").lower()
                    resultados = last_match.get("resultados") or []
                    if status_txt == "ok" and resultados:
                        return {"status": "ok", "fonte": "sbcp", "raw": last_match}
                    elif "não localizado" in status_txt or not resultados:
                        return {"status": "not_found", "fonte": "sbcp", "raw": last_match}
                    else:
                        return {"status": "error", "fonte": "sbcp", "raw": last_match, "stderr": out.stderr}
                else:
                    return {"status": "error", "fonte": "sbcp", "reason": "sem_registro_no_resultados_json", "stderr": out.stderr}
        except FileNotFoundError:
            return {"status": "error", "fonte": "sbcp", "reason": "resultados_json_nao_encontrado", "stderr": out.stderr}

    except subprocess.TimeoutExpired:
        return {"status": "error", "fonte": "sbcp", "reason": "timeout"}
    except Exception as e:
        return {"status": "error", "fonte": "sbcp", "reason": str(e)}


# ========= Loop principal =========
def main_loop():
    print("🧵 Worker de validação iniciado.", flush=True)
    print(f"Config: POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS} TIMEOUT={JOB_TIMEOUT_S}s FONTE_DEFAULT={FONTE_DEFAULT}", flush=True)

    while _running:
        try:
            with engine.begin() as conn:
                job = claim_job(conn)
                if not job:
                    # nada na fila — dormir e seguir
                    pass
                else:
                    result = run_validator(job.fonte, job.member_id, job.nome, job.email)

                    if result["status"] in ("ok", "not_found"):
                        finalize_job(conn, job.id, job.member_id, result["status"], result)
                    else:
                        # erro transitório: decide retry
                        if retry_or_fail(conn, job):
                            # apenas loga; finalize_job será chamado quando esgotar tentativas
                            if VERBOSE_LOG:
                                print(f"⚠️  Job {job.id}: erro transitório, programado retry.", flush=True)
                        else:
                            # excedeu tentativas → registra como FAILED
                            finalize_job(conn, job.id, job.member_id, "error", result)

        except SQLAlchemyError as e:
            print(f"💥 Erro de banco: {e}", flush=True)
        except Exception as e:
            print(f"💥 Erro inesperado: {e}", flush=True)

        time.sleep(POLL_SECONDS)

    print("👋 Worker finalizado com segurança.", flush=True)


if __name__ == "__main__":
    main_loop()
