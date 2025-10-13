# -*- coding: utf-8 -*-
"""
Scraper da SBCP (cirurgiaplastica.org.br) usando Playwright (API síncrona).

Fluxo:
1) Abre a página de busca.
2) Preenche o nome e clica em Buscar.
3) Clica no link "Perfil Completo" (modal; href="#0", class="cirurgiao-perfil-link").
4) Espera o modal aparecer e extrai pares <dt>/<dd> dentro de ".cirurgiao-info".
5) Normaliza CRM/RQE/CREFITO (ex.: "32019" e "32019-MG").

Pontos de robustez:
- Fecha banner de cookies se aparecer.
- Usa click com force/dispatch_event para contornar viewport.
- Espera explícita pelo modal com múltiplos seletores de fallback.
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"


# =========================
# Utilitários de normalização
# =========================
def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _num_uf(s: str) -> str:
    s = (s or "").upper().strip()
    if not s:
        return ""
    m = re.search(r"(\d+)\s*(?:[-/ ]\s*([A-Z]{2}))?", s)
    if not m:
        return _digits(s)
    num = m.group(1); uf = m.group(2)
    return f"{num}-{uf}" if uf else num

def _strip_accents_lower(s: str) -> str:
    import unicodedata
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s.rstrip(":")

def _split_multi_ids(v: Optional[str]) -> List[str]:
    if not v:
        return []
    found = re.findall(r"\d+\s*(?:[-/ ]\s*[A-Z]{2})?", v.upper())
    return [x.strip() for x in found] if found else [v.strip()]


# =========================
# Helpers de banco (opcionais)
# =========================
def get_conn() -> Optional["psycopg2.extensions.connection"]:
    if not psycopg2 or not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)

def log_validation(conn, member_id: int, fonte: str, status: str, payload: Dict[str, Any]) -> None:
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO validations_log (member_id, fonte, status, payload, created_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW())
            """,
            (member_id, fonte, status, json_dumps(payload)),
        )
    conn.commit()

def set_member_validation(conn, member_id: int, status: str, fonte: str) -> None:
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET status_validation = %s,
                   fonte_validation  = %s,
                   updated_at        = NOW()
             WHERE id = %s
            """,
            (status, fonte, member_id),
        )
    conn.commit()

def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# =========================
# Playwright helpers
# =========================
def _ensure_playwright_browsers(steps: List[str]) -> None:
    try:
        steps.append("try_playwright_install_check")
        with sync_playwright() as p:
            _ = p.chromium
        steps.append("playwright_ok")
    except Exception as e:
        steps.append(f"playwright_missing:{e}")
        try:
            import subprocess
            subprocess.run(
                ["python", "-m", "playwright", "install", "--with-deps", "chromium"],
                check=True,
                capture_output=True,
                text=True,
            )
            steps.append("chromium_installed")
        except Exception as e2:
            steps.append(f"chromium_install_error:{e2}")

def _try_select(page, selectors: List[str], timeout: int = 10000, steps: Optional[List[str]] = None):
    for css in selectors:
        try:
            loc = page.locator(css).first
            loc.wait_for(state="visible", timeout=timeout)
            if steps is not None:
                steps.append(f"found_selector:{css}")
            return loc
        except Exception:
            continue
    raise PWTimeout(f"Nenhum seletor correspondente ficou visível: {selectors}")

def _maybe_close_cookie_banner(page, steps: List[str]) -> None:
    candidates = [
        "#onetrust-accept-btn-handler",
        "button:has-text('Aceitar')",
        "button:has-text('Accept')",
        ".cli_action_button",
        ".cc-btn",
        "button:has-text('Ok')",
        "button:has-text('Fechar')",
    ]
    try:
        for sel in candidates:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                try:
                    loc.click(timeout=1500)
                    steps.append(f"cookie_banner_closed:{sel}")
                    break
                except Exception:
                    continue
    except Exception:
        pass


# =========================
# EXTRAÇÃO do perfil (lendo dt/dd)
# =========================
def _extract_profile(page, steps: List[str]) -> Dict[str, Any]:
    dados: Dict[str, Any] = {}

    # Nome
    try:
        nome_sel = page.locator("h1, h2, .perfil-nome, .titulo-perfil").first
        nome_txt = nome_sel.inner_text(timeout=3000).strip()
        if nome_txt:
            dados["nome"] = nome_txt
            steps.append("ok_nome")
    except Exception:
        steps.append("miss_nome")

    # Container principal
    container = page.locator(".cirurgiao-info").first
    try:
        container.wait_for(state="visible", timeout=5000)
    except Exception:
        steps.append("miss_cirurgiao_info")
        return dados

    # Pares dt/dd
    try:
        dt_texts = [t.strip() for t in container.locator("dt").all_inner_texts()]
        dd_texts = [t.strip() for t in container.locator("dd").all_inner_texts()]
    except Exception as e:
        steps.append(f"miss_dt_dd:{e}")
        return dados

    n = min(len(dt_texts), len(dd_texts))
    steps.append(f"pares_dt_dd={n}")

    raw_map: Dict[str, str] = {}
    for i in range(n):
        k_raw = dt_texts[i]
        v_raw = dd_texts[i]
        k = _strip_accents_lower(k_raw)
        raw_map[k] = v_raw
        dados.setdefault("_raw_pairs", []).append({"k": k_raw, "v": v_raw})

    # Aliases
    aliases = {
        "crm": ["crm", "registro crm", "crm/uf", "crm uf", "nº crm", "numero crm"],
        "rqe": ["rqe", "registro de qualificacao", "registro de qualificacao especialista",
                "registro de qualificação", "registro de qualificação especialista"],
        "crefito": ["crefito", "registro crefito", "nº crefito", "numero crefito"],
    }

    def first_by_alias(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in raw_map and raw_map[k]:
                return raw_map[k]
        return None

    crm_val = first_by_alias(aliases["crm"])
    rqe_val = first_by_alias(aliases["rqe"])
    crefito_val = first_by_alias(aliases["crefito"])

    crms = _split_multi_ids(crm_val)
    rqes = _split_multi_ids(rqe_val)
    crefitos = _split_multi_ids(crefito_val)

    if crms:
        dados["crm"] = crms[0]
        dados["crms"] = crms
        dados["crm_padrao"] = _num_uf(crms[0])
        dados["crms_padrao"] = [_num_uf(x) for x in crms]

    if rqes:
        dados["rqe"] = rqes[0]
        dados["rqes"] = rqes
        dados["rqe_padrao"] = _num_uf(rqes[0])
        dados["rqes_padrao"] = [_num_uf(x) for x in rqes]

    if crefitos:
        dados["crefito"] = crefitos[0]
        dados["crefitos"] = crefitos
        dados["crefito_padrao"] = _num_uf(crefitos[0])
        dados["crefitos_padrao"] = [_num_uf(x) for x in crefitos]

    try:
        html_snip = container.inner_html(timeout=2000)[:800]
        dados["_perfil_html_snippet"] = html_snip
        steps.append("perfil_html_snippet_ok")
    except Exception:
        steps.append("perfil_html_snippet_fail")

    dados["_raw_map"] = raw_map
    return dados


# =========================
# Abertura do modal "Perfil Completo"
# =========================
def _open_profile_modal(page, steps: List[str]) -> bool:
    """
    Procura e clica no link 'Perfil Completo' que abre o modal (href="#0", class="cirurgiao-perfil-link").
    Aguarda o modal renderizar a área '.cirurgiao-info'.
    """
    # 1) Espera por algum link de perfil
    perfil_selectors = [
        "a.cirurgiao-perfil-link[data-code]",
        "a.cirurgiao-perfil-link",
        "a:has-text('Perfil Completo')",
        "a:has-text('Perfil completo')",
        "a[href='#0'][data-code]"
    ]
    try:
        link = _try_select(page, perfil_selectors, timeout=30000, steps=steps)
    except PWTimeout:
        steps.append("perfil_link_nao_encontrado")
        return False

    # 2) Clique robusto (viewport/overlay)
    try:
        link.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        pass
    try:
        link.click(timeout=12000, force=True)
        steps.append("click_perfil_completo:force")
    except Exception as e:
        steps.append(f"click_force_fail:{e}")
        try:
            handle = link.element_handle(timeout=2000)
            if handle:
                page.dispatch_event("a.cirurgiao-perfil-link, a:has-text('Perfil Completo')", "click")
                steps.append("click_perfil_completo:dispatch_event")
            else:
                raise
        except Exception as e2:
            steps.append(f"click_dispatch_fail:{e2}")
            return False

    # 3) Espera o modal carregar (qualquer dos seletores abaixo)
    modal_candidates = [
        ".cirurgiao-info",             # conteúdo que queremos
        ".mfp-content .cirurgiao-info",
        ".modal .cirurgiao-info",
        "div[role='dialog'] .cirurgiao-info",
    ]
    try:
        page.wait_for_selector(", ".join(modal_candidates), timeout=30000, state="visible")
        steps.append("perfil_modal_ok")
        return True
    except Exception as e:
        steps.append(f"perfil_modal_timeout:{e}")
        return False


# =========================
# BUSCA e navegação
# =========================
def buscar_sbcp(member_id: Optional[int], nome: str, email: Optional[str] = None, steps: Optional[List[str]] = None) -> Dict[str, Any]:
    if steps is None:
        steps = []
    start = time.time()
    nome_busca = (nome or "").strip()

    if not nome_busca:
        return {
            "ok": False, "qtd": 0, "resultados": [], "steps": steps + ["nome_vazio"],
            "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
            "reason": "nome_vazio",
        }

    _ensure_playwright_browsers(steps)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(ignore_https_errors=True, java_script_enabled=True)
            page = context.new_page()
            page.set_default_timeout(30000)

            # 1) Abre página
            steps.append("launch chromium")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
            steps.append(f"abrindo {BASE_URL}")
            _maybe_close_cookie_banner(page, steps)

            # 2) Preenche campo de nome
            nome_selectors = [
                "input#cirurgiao_nome",
                "input[name='cirurgiao_nome']",
                "input[placeholder*='Nome']",
                "input[type='text']",
            ]
            try:
                nome_input = _try_select(page, nome_selectors, timeout=15000, steps=steps)
                nome_input.fill(nome_busca)
                steps.append(f"preencheu:input#cirurgiao_nome='{nome_busca}'")
            except PWTimeout:
                steps.append("falha_input_nome")
                return {
                    "ok": False, "qtd": 0, "resultados": [], "steps": steps,
                    "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_input",
                }

            # 3) Submete busca
            submit_selectors = [
                "input#cirurgiao_submit",
                "button#cirurgiao_submit",
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Buscar')",
            ]
            try:
                submit_btn = _try_select(page, submit_selectors, timeout=10000, steps=steps)
                submit_btn.click(timeout=12000)
                steps.append("clicou:input#cirurgiao_submit")
            except Exception:
                steps.append("falha_submit")
                return {
                    "ok": False, "qtd": 0, "resultados": [], "steps": steps,
                    "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_submit",
                }

            # 4) Aguarda rede assentar e tenta abrir o modal do primeiro perfil
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass

            if not _open_profile_modal(page, steps):
                steps.append("sem_resultados_ou_layout_alterado")
                return {
                    "ok": False, "qtd": 0, "resultados": [], "steps": steps,
                    "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
                    "reason": "sem_resultados_ou_layout_alterado",
                }

            # 5) Extrai via dt/dd e normaliza
            dados = _extract_profile(page, steps)
            qtd_detectada = 1 if dados else 0

            return {
                "ok": bool(dados), "qtd": qtd_detectada, "resultados": [],
                "dados": dados, "steps": steps, "nome_busca": nome_busca,
                "timing_ms": int((time.time() - start) * 1000),
            }

    except PWError as e:
        steps.append(f"playwright_error:{repr(e)}")
        return {
            "ok": False, "qtd": 0, "resultados": [], "steps": steps,
            "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
            "reason": "playwright_browser_missing",
        }
    except Exception as e:
        steps.append(f"erro_inesperado:{repr(e)}")
        return {
            "ok": False, "qtd": 0, "resultados": [], "steps": steps,
            "nome_busca": nome_busca, "timing_ms": int((time.time() - start) * 1000),
            "reason": "erro_inesperado",
        }


# =========================
# Execução direta para testes locais
# =========================
if __name__ == "__main__":
    import sys, json
    nome_arg = " ".join(sys.argv[1:]).strip() or "GUSTAVO AQUINO"
    out = buscar_sbcp(member_id=None, nome=nome_arg, email=None, steps=[])
    print(json.dumps(out, ensure_ascii=False, indent=2))
