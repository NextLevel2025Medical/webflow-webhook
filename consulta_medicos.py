# -*- coding: utf-8 -*-
"""
Consulta na SBCP com Playwright (headless) e grava o resultado no banco.
- Sem necessidade de Chromedriver.
- Registra passos de debug em `validations_log.payload` para diagnóstico.
ENV esperadas:
  DATABASE_URL     -> string de conexão Postgres (psycopg2)
  SBCP_URL         -> opcional (default: https://cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao)
"""

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from playwright.sync_api import Playwright, TimeoutError as PWTimeout, sync_playwright


SBCP_URL = os.getenv(
    "SBCP_URL",
    "https://cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao",
)
DATABASE_URL = os.getenv("DATABASE_URL")


# ---------- util DB ----------
def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def log_validation(
    conn: psycopg2.extensions.connection,
    member_id: int,
    fonte: str,
    status: str,
    payload: Dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO validations_log (member_id, fonte, status, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (member_id, fonte, status, json.dumps(payload, ensure_ascii=False)),
        )


def set_member_validation(
    conn: psycopg2.extensions.connection,
    member_id: int,
    status: str,
    fonte: Optional[str],
) -> None:
    """Atualiza a linha em membersnextlevel (colunas validacao_acesso, portal_validado)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET validacao_acesso = %s,
                   portal_validado  = %s
             WHERE id = %s
            """,
            (status, fonte, member_id),
        )


# ---------- scraping ----------
def _try_selector(page, steps: List[str], selectors: List[str], timeout: int = 6000):
    """Tenta uma lista de seletores até achar um existente; retorna o handle."""
    last_err = None
    for sel in selectors:
        try:
            steps.append(f"aguardando seletor: {sel}")
            handle = page.wait_for_selector(sel, timeout=timeout, state="visible")
            return handle
        except Exception as e:
            last_err = e
            steps.append(f"falhou seletor: {sel} ({type(e).__name__})")
    if last_err:
        raise last_err
    raise RuntimeError("Nenhum seletor válido encontrado")


def _extract_modal_info(page, steps: List[str]) -> Dict[str, Any]:
    """
    Lê o modal 'Perfil Completo' e monta um dicionário:
      {"nome":..., "cidade":..., "crm": "...", "crm2": "...", "rqe": "..."}
    Estrutura do modal (observada): blocos com dt/dd.
    """
    steps.append("extraindo informações do modal")
    info = page.evaluate(
        """
        () => {
          const data = {};
          // Nome no título
          const h3 = document.querySelector('div.cirurgiao-details h3');
          if (h3) data.nome = h3.textContent.trim();

          // Cidade
          const cidadeDT = Array.from(document.querySelectorAll('div.cirurgiao-info dt'))
            .find(dt => dt.textContent.trim().toUpperCase().startsWith('CIDADE'));
          if (cidadeDT) {
            const dd = cidadeDT.nextElementSibling;
            if (dd) data.cidade = dd.textContent.trim();
          }

          // CRM
          const crmDT = Array.from(document.querySelectorAll('div.cirurgiao-info dt'))
            .find(dt => dt.textContent.trim().toUpperCase().startsWith('CRM:'));
          if (crmDT) {
            const dd = crmDT.nextElementSibling;
            if (dd) data.crm = dd.textContent.trim();
          }

          // CRM 2
          const crm2DT = Array.from(document.querySelectorAll('div.cirurgiao-info dt'))
            .find(dt => dt.textContent.trim().toUpperCase().startsWith('CRM 2'));
          if (crm2DT) {
            const dd = crm2DT.nextElementSibling;
            if (dd) data.crm2 = dd.textContent.trim();
          }

          // RQE
          const rqeDT = Array.from(document.querySelectorAll('div.cirurgiao-info dt'))
            .find(dt => dt.textContent.trim().toUpperCase().startsWith('RQE'));
          if (rqeDT) {
            const dd = rqeDT.nextElementSibling;
            if (dd) data.rqe = dd.textContent.trim();
          }

          return data;
        }
        """
    )
    return info or {}


def buscar_sbcp(
    member_id: int,
    nome: str,
    email: str,
    steps: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Faz a busca na SBCP e retorna dict com:
      {
        "ok": bool,
        "resultados": [ {...} ],   // lista de perfis (apenas 1º detalhado)
        "steps": [...],
        "nome_busca": "...",
        "email": "...",
        "qtd": <int>,
        "timing_ms": <int>
      }
    """
    if steps is None:
        steps = []
    t0 = time.time()
    payload: Dict[str, Any] = {
        "nome_busca": nome,
        "email": email,
        "resultados": [],
        "qtd": 0,
        "steps": steps,
    }

    with sync_playwright() as p:
        browser = None
        try:
            steps.append("launch chromium")
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context(
                ignore_https_errors=True,
                java_script_enabled=True,
                viewport={"width": 1366, "height": 900},
            )
            page = context.new_page()

            steps.append(f"abrindo URL {SBCP_URL}")
            page.goto(SBCP_URL, wait_until="domcontentloaded", timeout=30000)

            # Campo nome (tentamos múltiplos seletores)
            nome_input = _try_selector(
                page,
                steps,
                [
                    'input#cirurgiao_nome',
                    'input[name="cirurgiao_nome"]',
                    'input[placeholder*="Nome"]',
                    'input[type="text"]',
                ],
            )
            steps.append("preenchendo nome")
            nome_input.fill("")
            nome_input.type(nome, delay=20)

            # Botão buscar
            try:
                steps.append("clicando botão (input#cirurgiao_submit)")
                page.click('input#cirurgiao_submit')
            except Exception:
                steps.append('fallback: clicando botão por texto "Buscar"')
                page.get_by_role("button", name="Buscar").click()

            # Espera resultados (item) OU estado de "sem resultados"
            try:
                steps.append("aguardando resultados (item)")
                page.wait_for_selector("div.cirurgiao-results-item", timeout=15000)
            except PWTimeout:
                steps.append("nenhum resultado visível; tentando detectar vazio")
                # Se não há itens, assume zero:
                payload["qtd"] = 0
                payload["ok"] = False
                payload["timing_ms"] = int((time.time() - t0) * 1000)
                return payload

            # Abre o primeiro "Perfil Completo"
            steps.append('clicando "Perfil Completo" do primeiro item')
            page.click("a.cirurgiao-perfil-link")

            steps.append("aguardando modal de perfil")
            page.wait_for_selector("div.mfp-content", timeout=15000)

            info = _extract_modal_info(page, steps)
            if info:
                payload["resultados"].append(info)
                payload["qtd"] = 1
                payload["ok"] = True
            else:
                payload["qtd"] = 0
                payload["ok"] = False

            payload["timing_ms"] = int((time.time() - t0) * 1000)
            return payload

        except Exception as e:
            steps.append(f"exception: {e}")
            payload["ok"] = False
            payload["qtd"] = 0
            payload["timing_ms"] = int((time.time() - t0) * 1000)
            return payload
        finally:
            try:
                if browser:
                    browser.close()
            except Exception:
                pass


# ---------- main CLI ----------
def main():
    """
    Execução direta:
      python consulta_medicos.py <member_id> "<nome>" "<email>"
    """
    if len(sys.argv) < 4:
        print('uso: python consulta_medicos.py <member_id> "<nome>" "<email>"')
        sys.exit(2)

    member_id = int(sys.argv[1])
    nome = sys.argv[2]
    email = sys.argv[3]

    steps: List[str] = []
    result = buscar_sbcp(member_id, nome, email, steps)

    conn = db()
    fonte = "sbcp"

    # Grava log detalhado
    status_log = "ok" if result.get("ok") else "error"
    log_validation(conn, member_id, fonte, status_log, {"raw": result, "fonte": fonte})

    # Atualiza estado do membro (aprovado/recusado)
    if result.get("ok"):
        set_member_validation(conn, member_id, "aprovado", fonte)
    else:
        # mantém pendente/recusado a critério; aqui vamos marcar como "recusado"
        set_member_validation(conn, member_id, "recusado", fonte)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
