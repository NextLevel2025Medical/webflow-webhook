# consulta_medicos.py
# Playwright headless + auto-install de Chromium no runtime.
# Alvos específicos do site da SBCP:
#   - input de nome:  #cirurgiao_nome, input[name='nome'], input[name='cirurgiao_nome']
#   - botão de buscar: input#cirurgiao_submit (type=submit, value=Buscar)
#   - form: form#cirurgiao-form

import os
import sys
import re
import json
import subprocess
from time import perf_counter
from typing import Dict, List, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

# ---------------- util de logging ----------------
def _log(steps: List[str], msg: str):
    steps.append(msg)

# ---------------- fallback de instalação ----------------
def _ensure_browser_once(steps: List[str]) -> bool:
    try:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
        _log(steps, "playwright install chromium (fallback runtime)…")
        proc = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=240
        )
        _log(steps, f"playwright install rc={proc.returncode}")
        if proc.stdout:
            _log(steps, f"install stdout: {proc.stdout[-400:]}")
        if proc.stderr:
            _log(steps, f"install stderr: {proc.stderr[-400:]}")
        return proc.returncode == 0
    except Exception as e:
        _log(steps, f"playwright install falhou: {e}")
        return False

# ---------------- automação SBCP ----------------
def buscar_sbcp(nome: str, email: str = "", steps: Optional[List[str]] = None) -> Dict:
    if steps is None:
        steps = []

    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"

    def _run_once() -> Dict:
        with sync_playwright() as pw:
            _log(steps, "launch chromium…")
            browser = pw.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(viewport={"width": 1366, "height": 900})
            page = context.new_page()
            try:
                _log(steps, f"goto {URL}")
                page.goto(URL, wait_until="domcontentloaded", timeout=35000)

                # Aguarda o formulário ou o botão existirem (o site carrega conteúdo depois do DOMContentLoaded)
                try:
                    _log(steps, "esperando form/btn: #cirurgiao_submit ou form#cirurgiao-form…")
                    page.wait_for_selector("#cirurgiao_submit, form#cirurgiao-form", timeout=20000, state="attached")
                except PWTimeout:
                    _log(steps, "timeout esperando form/btn — tentaremos mesmo assim.")

                # Fecha banners de cookies se aparecerem
                try:
                    cookie_btn = page.locator(
                        "text=/Aceitar|Concordo|OK|Fechar|Accept|Agree/i, "
                        "button#onetrust-accept-btn-handler, "
                        ".ot-sdk-container button[aria-label*='accept' i], "
                        ".cli-modal .wt-cli-accept-all-btn"
                    ).first
                    if cookie_btn.count() and cookie_btn.is_visible():
                        cookie_btn.click(timeout=1500)
                        _log(steps, "banner de cookies fechado.")
                except Exception:
                    _log(steps, "sem banner de cookies clicável.")

                # Campo de nome (prioriza o id do site)
                nome_sel = "#cirurgiao_nome, input[name='cirurgiao_nome'], input[name='nome']"
                nome_input = page.locator(nome_sel).first
                _log(steps, f"checando campo nome ({nome_sel}) count={nome_input.count()}")
                if not nome_input.count():
                    return {"status": "error", "reason": "campo_nome_nao_encontrado", "qtd": 0}

                _log(steps, f"preenchendo nome: {nome}")
                nome_input.fill(nome)

                # Botão Buscar: seletor exato da página (id)
                btn_sel_list = [
                    "#cirurgiao_submit",
                    "input[name='cirurgiao_submit']",
                    "input[type='submit'][value=/Buscar/i]",
                    "button[type='submit']",
                    "input[type='submit']",
                ]
                btn = None
                for sel in btn_sel_list:
                    loc = page.locator(sel).first
                    cnt = 0
                    try:
                        cnt = loc.count()
                    except Exception:
                        pass
                    _log(steps, f"procurando botão com '{sel}' count={cnt}")
                    if cnt and loc.is_visible():
                        btn = loc
                        break

                # Para diagnóstico: captura HTML parcial do form
                try:
                    form = page.locator("form#cirurgiao-form").first
                    if form and form.count():
                        snippet = form.evaluate("el => el.outerHTML.slice(0,800)")
                        _log(steps, f"form snippet: {snippet}")
                except Exception:
                    pass

                submetido = False
                if btn:
                    _log(steps, "clicando botão #cirurgiao_submit…")
                    try:
                        btn.scroll_into_view_if_needed(timeout=1500)
                    except Exception:
                        pass
                    try:
                        btn.click(timeout=4000)
                        submetido = True
                    except Exception as e:
                        _log(steps, f"click direto falhou: {e}")

                # Fallback 1: click via JS
                if not submetido:
                    try:
                        _log(steps, "fallback JS: document.getElementById('cirurgiao_submit').click()")
                        page.evaluate("""() => {
                            const b = document.getElementById('cirurgiao_submit');
                            if (b) b.click();
                        }""")
                        submetido = True
                    except Exception as e:
                        _log(steps, f"fallback JS click falhou: {e}")

                # Fallback 2: Enter no input
                if not submetido:
                    try:
                        _log(steps, "fallback: Enter no campo de nome")
                        nome_input.press("Enter")
                        submetido = True
                    except Exception as e:
                        _log(steps, f"Enter falhou: {e}")

                # Fallback 3: submit() do form
                if not submetido:
                    try:
                        _log(steps, "fallback: form.submit()")
                        page.evaluate("""() => {
                            const f = document.querySelector('form#cirurgiao-form') || document.querySelector('form');
                            if (f) f.submit();
                        }""")
                        submetido = True
                    except Exception as e:
                        _log(steps, f"form.submit() falhou: {e}")

                page.wait_for_timeout(1200)
                # Aguarda aparição de “Perfil Completo” (indicador de resultados)
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    _log(steps, "timeout aguardando 'Perfil Completo' (pode ser zero resultados).")

                qtd = page.locator("text=/Perfil Completo/i").count()
                _log(steps, f"qtd_perfil_completo={qtd}")

                return {"status": ("ok" if qtd > 0 else "not_found"), "qtd": qtd}

            finally:
                try:
                    context.close()
                    browser.close()
                except Exception:
                    pass

    # Tenta rodar; se faltar binário, instala e tenta novamente
    try:
        return _run_once()
    except Exception as e:
        msg = str(e)
        _log(steps, f"launch/navegação falhou: {msg}")
        if "Executable doesn't exist" in msg or "playwright install" in msg:
            _ensure_browser_once(steps)
            return _run_once()
        raise

# ---------------- main/CLI ----------------
def main():
    # argv: member_id, nome, (celular), email, created_at
    member_id = None
    nome = ""
    email = ""
    if len(sys.argv) >= 2:
        try:
            member_id = int(sys.argv[1])
        except Exception:
            member_id = None
    if len(sys.argv) >= 3:
        nome = sys.argv[2]
    if len(sys.argv) >= 5:
        email = sys.argv[4]

    steps: List[str] = []
    t0 = perf_counter()

    try:
        res = buscar_sbcp(nome=nome, email=email, steps=steps)
        payload = {
            "member_id": member_id,
            "nome_busca": nome,
            "email": email,
            "qtd": res.get("qtd", 0),
            "resultados": [],
            "debug": {"steps": steps[-250:]},
        }
        out = {"status": res["status"], "fonte": "sbcp", "raw": payload}
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        out = {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {
                "member_id": member_id,
                "nome_busca": nome,
                "email": email,
                "qtd": 0,
                "resultados": [],
                "debug": {"steps": steps[-250:]},
            },
        }
        print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
