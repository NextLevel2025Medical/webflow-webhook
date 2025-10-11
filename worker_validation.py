# -*- coding: utf-8 -*-
"""
Worker que consome validation_jobs:
- Pega o próximo job com status PENDING (com lock).
- Marca RUNNING, incrementa attempts.
- Executa validação (SBCP) chamando consulta_medicos.buscar_sbcp(...)
- Grava resultado em validations_log e atualiza membersnextlevel.
- Atualiza status do job para SUCCEEDED / FAILED.
ENV:
  DATABASE_URL
  POLL_SECONDS  (default 3)
  MAX_ATTEMPTS  (default 3)
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

from consulta_medicos import buscar_sbcp, log_validation, set_member_validation


DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))


def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def fetch_next_job(conn) -> Optional[Dict[str, Any]]:
    """
    Seleciona 1 job PENDING com SKIP LOCKED para evitar concorrência.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, member_id, email, nome, fonte, status, attempts
              FROM validation_jobs
             WHERE status = 'PENDING'
             ORDER BY id
             FOR UPDATE SKIP LOCKED
             LIMIT 1
            """
        )
        row = cur.fetchone()
        return dict(row) if row else None


def mark_running(conn, job_id: int, attempts: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validation_jobs
               SET status = 'RUNNING',
                   attempts = %s
             WHERE id = %s
            """,
            (attempts + 1, job_id),
        )


def finalize_job(conn, job_id: int, status: str, last_error: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE validation_jobs
               SET status = %s,
                   last_error = %s
             WHERE id = %s
            """,
            (status, last_error, job_id),
        )


def work_loop():
    print(
        f"🧵 Worker de validação iniciado.\nConfig: POLL_SECONDS={POLL_SECONDS} MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )
    conn = db()

    while True:
        try:
            # Tenta pegar 1 job
            with conn:
                job = fetch_next_job(conn)

                if not job:
                    time.sleep(POLL_SECONDS)
                    continue

                job_id = job["id"]
                member_id = job["member_id"]
                email = job.get("email") or ""
                nome = job.get("nome") or ""
                fonte = job.get("fonte") or "sbcp"
                attempts = int(job.get("attempts") or 0)

                if attempts >= MAX_ATTEMPTS:
                    finalize_job(conn, job_id, "FAILED", "tentativas_excedidas")
                    continue

                print(f"⚙️  Job {job_id} -> RUNNING (attempt {attempts + 1}) [member_id={member_id} fonte={fonte}]")
                mark_running(conn, job_id, attempts)

            # Executa fora do bloco de transação longa
            steps: List[str] = []
            result = {}
            last_error = None

            try:
                if fonte == "sbcp":
                    result = buscar_sbcp(member_id, nome, email, steps)
                else:
                    steps.append(f"fonte_desconhecida: {fonte}")
                    result = {"ok": False, "steps": steps, "resultados": [], "qtd": 0}

                status_log = "ok" if result.get("ok") else "error"

            except Exception as e:
                last_error = f"exception:{e}"
                result = {"ok": False, "steps": steps + [last_error], "resultados": [], "qtd": 0}
                status_log = "error"

            # Grava log e atualiza membro conforme resultado
            try:
                log_validation(conn, member_id, fonte, status_log, {"raw": result, "fonte": fonte})
                if result.get("ok"):
                    set_member_validation(conn, member_id, "aprovado", fonte)
                else:
                    set_member_validation(conn, member_id, "pendente", fonte)
            except Exception as e:
                last_error = f"db_erro:{e}"

            # Finaliza o job
            with conn:
                if result.get("ok"):
                    finalize_job(conn, job_id, "SUCCEEDED", None)
                    print(f"✅ Job {job_id} -> SUCCEEDED (membro {member_id}: aprovado/{fonte})")
                else:
                    # Se ainda há tentativas disponíveis, volta para PENDING para retry
                    if attempts + 1 < MAX_ATTEMPTS:
                        finalize_job(conn, job_id, "PENDING", last_error or "retry")
                        print(f"🔁 Job {job_id} re-enfileirado (retry).")
                    else:
                        finalize_job(conn, job_id, "FAILED", last_error or "erro_definitivo")
                        print(f"🧯 Job {job_id} -> FAILED definitivo.")

        except Exception as outer:
            # Erro geral do loop
            print(f"💥 Loop erro: {outer}")
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    work_loop()
