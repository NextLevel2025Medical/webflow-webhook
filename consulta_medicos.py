# -*- coding: utf-8 -*-
"""
Scraper da SBCP (cirurgiaplastica.org.br) usando Playwright (API síncrona).

Principais funções públicas:
- buscar_sbcp(member_id, nome, email, steps): executa a busca pelo nome e tenta abrir o 1º perfil.
- log_validation(conn, member_id, fonte, status, payload): insere em validations_log.
- set_member_validation(conn, member_id, status, fonte): atualiza membersnextlevel.

Melhorias:
- Busca tolerante (múltiplos seletores; timeouts maiores).
- Diagnósticos claros em caso de falha (reason: falha_input, falha_submit,
  sem_resultados_ou_layout_alterado, falha_abrir_perfil, playwright_browser_missing).
- Extração de crm/rqe/crefito com normalização "*_padrao" (ex.: "98675-MG" ou "98675").
- Fallback de instalação automática do Chromium (opcional).
"""

import os
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

# ✅ Imports públicos (sem módulos _impl internos)
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

# =========================
# Conexão BD
# =========================
def db() -> psycopg2.extensions.connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

# =========================
# Helpers DB públicos
# =========================
def log_validation(conn, member_id: int, fonte: str, status: str, payload: Dict[str, Any]) -> None:
    """Insere um registro em validations_log."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO validations_log (member_id, fonte, status, payload, created_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW())
            """,
            (member_id, fonte, status, psycopg2.extras.Json(payload)),
        )

def set_member_validation(conn, member_id: int, status: str, fonte: str) -> None:
    """Atualiza o membro com o resultado da validação."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET validacao_acesso = %s,
                   portal_validado  = %s,
                   validacao_at     = NOW()
             WHERE id = %s
            """,
            (status, fonte, member_id),
        )

# =========================
# Playwright: auto-heal (opcional)
# =========================
def _ensure_playwright_browsers(steps: List[str]) -> bool:
    """Tenta instalar o Chromium e deps caso o executável não exista."""
    import subprocess, sys
    try:
        r = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "--with-deps", "chromium"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        steps.append("playwright_install_ok")
        if r.stdout:
            steps.append(f"playwright_install_stdout={r.stdout.decode(errors='ignore')[:300]}")
        if r.stderr:
            steps.append(f"playwright_install_stderr={r.stderr.decode(errors='ignore')[:300]}")
        return True
    except Exception as e:
        steps.append(f"playwright_install_fail:{e}")
        return False

# =========================
# Helpers de Playwright
# =========================
def _try_click_cookies(page, steps: List[str]) -> None:
    """Tenta aceitar cookies ou fechar banners comuns (best-effort)."""
    candidatos = [
        "button:has-text('Aceitar')",
        "button:has-text('Concordo')",
        "button:has-text('OK')",
        "text=Aceitar cookies",
        "#onetrust-accept-btn-handler",
        "button#onetrust-accept-btn-handler",
    ]
    for sel in candidatos:
        try:
            page.locator(sel).first.click(timeout=1500)
            steps.append(f"cookies_click:{sel}")
            break
        except Exception:
            continue

def _fill_by_many_selectors(page, selectors: List[str], value: str, steps: List[str], timeout_ms: int = 8000) -> bool:
    for sel in selectors:
        try:
            page.locator(sel).first.fill(value, timeout=timeout_ms)
            steps.append(f"preencheu:{sel}='{value}'")
            return True
        except Exception:
            steps.append(f"falha_preencher:{sel}")
    return False

def _click_by_many_selectors(page, selectors: List[str], steps: List[str], timeout_ms: int = 8000) -> bool:
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=timeout_ms)
            steps.append(f"clicou:{sel}")
            return True
        except Exception:
            steps.append(f"falha_click:{sel}")
    return False

# =========================
# Extração de perfil
# =========================
def _extract_profile(page, steps: List[str]) -> Dict[str, Any]:
    """
    Extrai dados básicos do primeiro perfil aberto.
    Ajuste os seletores abaixo conforme a estrutura atual do site.
    """
    dados: Dict[str, Any] = {}

    campos = [
        ("nome", "h1, h2, .perfil-nome, .titulo-perfil"),
        ("crm", "text=CRM"),
        ("rqe", "text=RQE"),
        ("crefito", "text=CREFITO"),
    ]

    for key, sel in campos:
        try:
            el = page.locator(sel).first
            txt = el.inner_text(timeout=3000)
            dados[key] = txt.strip()
            steps.append(f"ok_{key}")
        except Exception:
            steps.append(f"miss_{key}")

    # Normalizações simples (padrao = apenas dígitos ou dígitos-UF se houver)
    import re as _re

    def _digits(s: Optional[str]) -> str:
        return _re.sub(r"\D", "", s or "")

    def _num_uf(s: Optional[str]) -> str:
        s = (s or "").upper().strip()
        if not s:
            return ""
        m = _re.search(r"(\d+)\s*[-/ ]\s*([A-Z]{2})", s)
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        d = _digits(s)
        return d

    if "crm" in dados:
        dados["crm_padrao"] = _num_uf(dados["crm"])
    if "rqe" in dados:
        dados["rqe_padrao"] = _num_uf(dados["rqe"])
    if "crefito" in dados:
        dados["crefito_padrao"] = _num_uf(dados["crefito"])

    return dados

# =========================
# Busca no site (SBCP)
# =========================
def buscar_sbcp(member_id: int, nome_busca: str, email: str, steps: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Busca por nome no portal da SBCP (cirurgiaplastica.org.br) e tenta abrir o primeiro perfil.
    Retorno padrão:
      {
        ok: bool,
        qtd: int,
        resultados: [ { ... perfil ... } ],
        steps: [...],
        nome_busca: str,
        timing_ms: int,
        reason?: str
      }
    """
    steps = steps or []
    start = time.time()

    steps.append(f"wrapper: usando apenas nome='{nome_busca}' (args extras ignorados)")

    with sync_playwright() as p:
        # --- launch com retry/auto-heal ---
        try:
            browser = p.chromium.launch(headless=True)
        except PWError as e:
            steps.append(f"chromium_launch_fail:{e}")
            if _ensure_playwright_browsers(steps):
                # tenta relançar uma vez
                browser = p.chromium.launch(headless=True)
            else:
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "playwright_browser_missing",
                }

        context = browser.new_context()
        page = context.new_page()

        try:
            steps.append("launch chromium")
            page.goto(BASE_URL, timeout=30000, wait_until="domcontentloaded")
            steps.append(f"abrindo {BASE_URL}")

            _try_click_cookies(page, steps)

            # Preenche campo de nome e clica buscar
            filled = _fill_by_many_selectors(
                page,
                selectors=[
                    "input#cirurgiao_nome",
                    "input[name='cirurgiao_nome']",
                    "input[type='text']",
                ],
                value=nome_busca,
                steps=steps,
                timeout_ms=12000,
            )
            if not filled:
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps + ["nao_conseguiu_preencher_nome"],
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_input",
                }

            clicked = _click_by_many_selectors(
                page,
                selectors=[
                    "input#cirurgiao_submit",
                    "button:has-text('Buscar')",
                    "button[type='submit']",
                ],
                steps=steps,
                timeout_ms=12000,
            )
            if not clicked:
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps + ["nao_conseguiu_clicar_buscar"],
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_submit",
                }

            # Espera algum resultado aparecer (tolerante)
            candidatos = [
                ".cirurgiao-perfil-link",
                "a:has-text('Perfil Completo')",
                "a:has-text('perfil completo')",
                "a:has-text('Perfil')",
                "a[href*='perfil']",
                ".resultado-busca a",
            ]
            apareceu = False
            for sel in candidatos:
                try:
                    page.locator(sel).first.wait_for(timeout=25000, state="visible")
                    steps.append(f"resultado_apareceu:{sel}")
                    apareceu = True
                    break
                except PWTimeout:
                    steps.append(f"aguardou_sel_timeout:{sel}")
                    continue

            if not apareceu:
                # tenta identificar "sem resultados" e capturar snippet do HTML
                try:
                    msg_empty = page.get_by_text("Nenhum resultado", exact=False)
                    msg_empty.wait_for(timeout=2000, state="visible")
                    steps.append("sem_resultados_explicito")
                except Exception:
                    steps.append("nao_detectou_texto_sem_resultados")
                try:
                    html = page.locator("body").inner_html(timeout=2000)
                    steps.append(f"html_snippet={html[:600]}")
                except Exception:
                    pass
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "sem_resultados_ou_layout_alterado",
                }

            # Abre o primeiro perfil
            try:
                page.locator(", ".join(candidatos)).first.click(timeout=12000)
                steps.append("abriu_perfil_primeiro_resultado")
            except Exception as e:
                steps.append(f"falha_abrir_perfil:{e}")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_abrir_perfil",
                }

            # Extrai dados do perfil
            perfil = _extract_profile(page, steps)
            resultados = [perfil] if perfil else []

            return {
                "ok": bool(resultados),
                "qtd": len(resultados),
                "resultados": resultados,
                "steps": steps,
                "nome_busca": nome_busca,
                "timing_ms": int((time.time() - start) * 1000),
            }

        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
