# worker_validation.py
import os
import time
import json
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

# =========================
# Config
# =========================
def _env(name, default=None):
    v = os.environ.get(name)
    return v if v not in (None, "") else default

DATABASE_URL = (
    _env("DATABASE_URL")
    or f"postgresql://{_env('PGUSER','neon')}:{_env('PGPASSWORD','password')}@{_env('PGHOST','localhost')}:{_env('PGPORT','5432')}/{_env('PGDATABASE','neondb')}"
)

POLL_INTERVAL_SECONDS = int(_env("POLL_INTERVAL_SECONDS", "5"))
BATCH_SIZE = int(_env("BATCH_SIZE", "5"))

# =========================
# DB helpers
# =========================
@contextmanager
def db() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.close()
    except Exception:
        try:
            conn.close()
        except:
            pass
        raise

def log(*args, **kwargs):
    msg = " ".join([str(a) for a in args])
    if kwargs:
        msg += " " + " ".join([f"{k}={v}" for k, v in kwargs.items()])
    print(msg, flush=True)

def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

# =========================
# Validadores (sites)
# =========================
def buscar_sbcp(nome: str):
    """
    Valida RQE no site da SBCP (cirurgiaplastica.org.br).
    Retorno padronizado:
      {
        "ok": True|False,            # se achou um perfil compatível com o nome
        "numero": "12345" or "",     # número extraído (RQE/CRM/CREFITO) se encontrado
        "site": "cirurgiaplastica.org.br",
        "steps": [ ... strings de log ... ]
      }
    """
    steps = []
    site = "cirurgiaplastica.org.br"

    # >>>>>>>>>>>> TROQUE ESTE BLOCO PELO SEU CRAWLER REAL <<<<<<<<<<<<
    # Mock simples: se o nome tiver "Teste", retorna mismatch; se "GUSTAVO AQUINO", retorna 1364 por exemplo.
    nm = (nome or "").strip().upper()
    numero = ""
    ok = False
    if nm:
        # simular que achou perfil
        ok = True
        # simulador: número "buscado"
        if "GUILHERME" in nm or "GUSTAVO" in nm:
            numero = "1364"
        else:
            numero = "99999"
    steps.append(f"mock: nome='{nome}' ok={ok} numero='{numero}'")
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    return {"ok": ok, "numero": numero, "site": site, "steps": steps}

# Adicione aqui os próximos validadores na ordem desejada
VALIDATORS_IN_ORDER = [
    buscar_sbcp,
    # ex.: buscar_cfm, buscar_cremesp, ...
]

# =========================
# Core
# =========================
def fetch_pending_jobs(conn, limit=BATCH_SIZE):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT id, member_id, email, nome, fonte, status, attempts, created_at
        FROM public.validations_jobs
        WHERE status = 'PENDING'
        ORDER BY created_at ASC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return rows

def mark_running(conn, job_id):
    cur = conn.cursor()
    cur.execute("""
        UPDATE public.validations_jobs
        SET status = 'RUNNING'
        WHERE id = %s AND status = 'PENDING'
    """, (job_id,))
    updated = cur.rowcount
    cur.close()
    return updated == 1

def read_member_and_number(conn, member_id):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT id, nome, email,
               COALESCE(metadata, '{}'::jsonb) as metadata
        FROM public.membersnextlevel
        WHERE id = %s
    """, (member_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        return None, None

    meta = row["metadata"] or {}
    # aceite RQE/CRM/CREFITO; normalizamos tudo para dígitos
    numero = (
        meta.get("rqe") or meta.get("RQE") or
        meta.get("crm") or meta.get("CRM") or
        meta.get("crefito") or meta.get("CREFITO") or ""
    )
    return row, digits_only(numero)

def finish_success(conn, job_id, member_id, site_name, attempts_inc=1):
    # job
    cur = conn.cursor()
    cur.execute("""
        UPDATE public.validations_jobs
        SET status = 'SUCCEEDED',
            attempts = attempts + %s,
            fonte = %s
        WHERE id = %s
    """, (attempts_inc, site_name, job_id))
    # member
    cur.execute("""
        UPDATE public.membersnextlevel
        SET validacao_acesso = 'granted',
            portal_validado  = %s
        WHERE id = %s
    """, (site_name, member_id))
    cur.close()

def finish_failure(conn, job_id, member_id, attempts_inc=1):
    # job
    cur = conn.cursor()
    cur.execute("""
        UPDATE public.validations_jobs
        SET status = 'FAILED',
            attempts = attempts + %s,
            fonte = ''
        WHERE id = %s
    """, (attempts_inc, job_id))
    # member em pending e sem portal_validado
    cur.execute("""
        UPDATE public.membersnextlevel
        SET validacao_acesso = 'pending',
            portal_validado  = NULL
        WHERE id = %s
    """, (member_id,))
    cur.close()

def log_attempt(conn, member_id, fonte, status, payload_dict):
    # opcional: guarda auditoria de tentativas
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.validations_log (member_id, fonte, status, payload, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (member_id, fonte, status, json.dumps(payload_dict, ensure_ascii=False)))
        cur.close()
    except Exception as e:
        log("warn:failed to log validations_log", err=str(e))

def process_job(conn, job):
    job_id = job["id"]
    member_id = job["member_id"]
    nome = job["nome"]
    email = job["email"]

    member, numero_cadastro = read_member_and_number(conn, member_id)
    if not member:
        log("job-skipped: member not found", job_id=job_id, member_id=member_id)
        # marca como failed para não travar fila
        finish_failure(conn, job_id, member_id, attempts_inc=1)
        return

    log("RUN job", job_id=job_id, member_id=member_id, nome=nome, email=email, numero=numero_cadastro)

    # Caso não tenha número no cadastro, não há como confirmar → failure (ou deixe PENDING para tratar manualmente)
    if not numero_cadastro:
        log_attempt(conn, member_id, "", "no-number", {"reason": "member.metadata without rqe/crm/crefito"})
        finish_failure(conn, job_id, member_id, attempts_inc=1)
        return

    # tenta cada site em ordem até o primeiro que CONFIRMAR MESMO NÚMERO
    for validator in VALIDATORS_IN_ORDER:
        site_result = {}
        try:
            site_result = validator(nome or "")
        except Exception as e:
            log_attempt(conn, member_id, "", "validator-error", {"site": validator.__name__, "error": str(e)})
            continue

        site_name = site_result.get("site") or ""
        numero_site = digits_only(site_result.get("numero") or "")
        ok_lookup = bool(site_result.get("ok"))

        log("site-try", site=site_name, ok_lookup=ok_lookup, numero_site=numero_site)

        # se achou perfil e tem número, comparamos
        if ok_lookup and numero_site:
            if numero_site == numero_cadastro:
                # VALIDADO!
                log_attempt(conn, member_id, site_name, "matched", site_result)
                finish_success(conn, job_id, member_id, site_name, attempts_inc=1)
                return
            else:
                # achou, mas número difere → continua tentando outro site
                log_attempt(conn, member_id, site_name, "mismatch", {"site": site_name, "expected": numero_cadastro, "found": numero_site, "steps": site_result.get("steps")})
                continue
        else:
            # não achou/sem número → continua tentando
            log_attempt(conn, member_id, site_name, "not-found", site_result)
            continue

    # terminou todos os sites sem confirmar
    finish_failure(conn, job_id, member_id, attempts_inc=1)

def main_loop():
    log("worker-started")
    while True:
        try:
            with db() as conn:
                conn.autocommit = False

                jobs = fetch_pending_jobs(conn, limit=BATCH_SIZE)
                if not jobs:
                    conn.commit()
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                for job in jobs:
                    try:
                        if not mark_running(conn, job["id"]):
                            # outro worker pegou
                            conn.commit()
                            continue

                        process_job(conn, job)
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        log("job-error", job_id=job["id"], err=str(e))
                        # marca como FAILED para não travar
                        try:
                            with conn:
                                finish_failure(conn, job["id"], job["member_id"], attempts_inc=1)
                                conn.commit()
                        except Exception as e2:
                            log("job-error-fallback", err=str(e2))
        except Exception as e:
            log("loop-error", err=str(e))
            time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main_loop()
