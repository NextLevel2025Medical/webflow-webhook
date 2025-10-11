# consulta_medicos.py
# Playwright headless + fallback de instalação do Chromium em runtime.
# Compatível com:
#   - uso como função:   from consulta_medicos import buscar_sbcp
#   - uso via CLI:       python consulta_medicos.py <member_id> "<nome>" "" "<email>" "<created_at>"
#
# Saída (JSON no stdout quando usado via CLI):
# {
#   "status": "ok" | "not_found" | "error",
#   "fonte": "sbcp",
#   "reason": "...(se houver)...",
#   "raw": {
#       "member_id": 1364,
#       "nome_busca": "GUSTAVO AQUINO",
#       "email": "dr@exemplo.com",
#       "qtd": 1,
#       "resultados": [],
#       "timing_ms": 2310,
#       "tried_install": true,
#       "debug": { "steps": ["...", "..."] }
#       # opcional (descomentar no código onde indicado):
#       # "screenshot_b64": "...."
#   }
# }

import os
import sys
import re
import json
import base64
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
    """
    Garante o Chromium no ambiente (sem --with-deps).
    Usa cache persistente do Render em /opt/render/.cache/ms-playwright.
    """
    try:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
        _log(steps, "playwright install chromium (fallback runtime)…")
        proc = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=240
        )
        _log(steps, f"playwright install rc={proc.returncode}")
        if proc.stdout:
            _log(steps, f"install stdout: {proc.stdout[-500:]}")
        if proc.stderr:
            _log(steps, f"install stderr: {proc.stderr[-500:]}")
        return proc.returncode == 0
    except Exception as e:
        _log(steps, f"playwright install falhou: {e}")
        return False


# ---------------- automação SBCP ----------------
def buscar_sbcp(nome: str, email: str = "", steps: Optional[List[str]] = None) -> Dict:
    """
    1) Tenta abrir o Chromium. Se faltar binário, instala e tenta novamente (1x).
    2) Abre a página, preenche o nome, clica em 'Buscar' (ou equivalentes).
       - tenta fechar banner de cookies
       - procura botão em página raiz e iframes
       - se não achar, usa Enter no input ou form.submit()
    3) Considera 'ok' se aparecer ao menos um 'Perfil Completo' na listagem.
    Retorna dict com 'status', 'qtd' e logs em steps.
    """
    if steps is None:
        steps = []

    # preferir cache do Render (evita baixar em cada boot)
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
    tried_install = False

    def _run_once() -> Dict:
        with sync_playwright() as pw:
            _log(steps, "launch chromium…")
            browser = pw.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(viewport={"width": 1366, "height": 900})
            page = context.new_page()
            try:
                _log(steps, f"goto {URL}")
                page.goto(URL, wait_until="domcontentloaded", timeout=30000)

                # Campo de nome (algumas variações de seletor para robustez)
                _log(steps, "localizando campo de nome…")
                nome_input = page.locator("input[name='nome'], #cirurgiao_nome, input#nome").first
                if not nome_input.count():
                    _log(steps, "ERRO: campo de nome não encontrado")
                    return {"status": "error", "reason": "campo_nome_nao_encontrado", "qtd": 0}

                _log(steps, f"preenchendo nome: {nome}")
                nome_input.fill(nome)

                # ===== Patch robusto para encontrar/submeter a busca =====

                # 1) tenta fechar banner de cookies (vários rótulos comuns)
                try:
                    _log(steps, "checando banner de cookies…")
                    cookie_btn = page.locator(
                        "text=/Aceitar|Concordo|OK|Fechar|Accept|Agree/i, "
                        "button#onetrust-accept-btn-handler, "
                        ".ot-sdk-container button[aria-label*='accept' i], "
                        ".cli-modal .cli_settings_button, .cli-modal .wt-cli-accept-all-btn"
                    ).first
                    if cookie_btn and cookie_btn.count() and cookie_btn.is_visible():
                        cookie_btn.click(timeout=1000)
                        _log(steps, "banner de cookies fechado.")
                except Exception:
                    _log(steps, "nenhum banner de cookies clicável.")

                # helper para achar o botão em uma page/frame
                def _find_buscar(ctx):
                    candidatos = [
                        "text=/\\bBuscar\\b/i",
                        "text=/Pesquisar/i",
                        "text=/Procurar/i",
                        "button[type='submit']",
                        "input[type='submit']",
                        "#cirurgiao_submit",
                        "input[value=/Buscar|Pesquisar|Procurar/i]",
                    ]
                    # ARIA role com regex
                    try:
                        by_role = ctx.get_by_role("button", name=re.compile(r"(buscar|pesquisar|procurar)", re.I))
                        if by_role.count():
                            return by_role.first
                    except Exception:
                        pass
                    # CSS/Text candidatos
                    for sel in candidatos:
                        loc = ctx.locator(sel).first
                        try:
                            if loc.count() and loc.is_visible():
                                return loc
                        except Exception:
                            continue
                    return None

                _log(steps, "localizando botão de busca…")
                btn_buscar = _find_buscar(page)

                # 2) se não achou na página principal, varre iframes
                if not btn_buscar:
                    _log(steps, "não achou na raiz; procurando em iframes…")
                    for fr in page.frames:
                        try:
                            if fr == page.main_frame:
                                continue
                            cand = _find_buscar(fr)
                            if cand:
                                btn_buscar = cand
                                _log(steps, "botão encontrado dentro de iframe.")
                                break
                        except Exception:
                            continue

                # 3) aciona a busca (click | Enter | form.submit)
                submetido = False
                if btn_buscar:
                    _log(steps, "clicando em Buscar…")
                    try:
                        btn_buscar.scroll_into_view_if_needed(timeout=1500)
                    except Exception:
                        pass
                    try:
                        btn_buscar.click(timeout=3000)
                        submetido = True
                    except Exception as e:
                        _log(steps, f"click falhou: {e}; tentando via Enter…")

                if not submetido:
                    try:
                        _log(steps, "submetendo via Enter no campo de nome…")
                        nome_input.press("Enter")
                        submetido = True
                    except Exception as e:
                        _log(steps, f"Enter falhou: {e}; tentando form.submit()…")
                        try:
                            form = page.locator("form").first
                            if form and form.count():
                                form.evaluate("el => el.submit()")
                                submetido = True
                        except Exception as e2:
                            _log(steps, f"form.submit() falhou: {e2}")

                # 4) aguarda e coleta resultados
                page.wait_for_timeout(1200)
                _log(steps, "aguardando resultados (Perfil Completo)…")
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    _log(steps, "timeout esperando 'Perfil Completo' — pode ser zero resultados.")

                qtd = page.locator("text=/Perfil Completo/i").count()
                _log(steps, f"qtd_perfil_completo={qtd}")

                # Screenshot opcional (descomente se quiser salvar no payload)
                # try:
                #     if qtd == 0:
                #         snap = page.screenshot(full_page=True)
                #         screenshot_b64 = base64.b64encode(snap).decode("ascii")
                #         _log(steps, "anexando screenshot base64 (sem resultados)…")
                #         return {"status": ("ok" if qtd > 0 else "not_found"), "qtd": qtd, "screenshot_b64": screenshot_b64}
                # except Exception:
                #     pass

                return {"status": ("ok" if qtd > 0 else "not_found"), "qtd": qtd}

            finally:
                try:
                    context.close()
                    browser.close()
                except Exception:
                    pass

    # primeira tentativa
    try:
        return _run_once()
    except Exception as e:
        msg = str(e)
        _log(steps, f"launch/navegação falhou: {msg}")
        # se for falta de executável, tenta instalar e rodar de novo
        if "Executable doesn't exist" in msg or "playwright install" in msg:
            tried_install = _ensure_browser_once(steps)
            res2 = _run_once()
            res2["tried_install"] = tried_install
            return res2
        # outros erros sobem para quem chamou
        raise


# ---------------- main/CLI ----------------
def main():
    # Aceita 5 args do worker, mas só usa member_id, nome, email
    #   argv[1]=member_id  argv[2]=nome  argv[3]=celular  argv[4]=email  argv[5]=created_at
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
        t1 = perf_counter()

        payload = {
            "member_id": member_id,
            "nome_busca": nome,
            "email": email,
            "qtd": res.get("qtd", 0),
            "resultados": [],                 # não expandimos cartões/links; suficiente p/ validação
            "timing_ms": int((t1 - t0) * 1000),
            "tried_install": bool(res.get("tried_install", False)),
            "debug": {"steps": steps[-200:]}, # limita tamanho do log salvo no DB
        }

        # Se você habilitar screenshot no bloco acima, una aqui:
        if "screenshot_b64" in res:
            payload["screenshot_b64"] = res["screenshot_b64"]

        out = {
            "status": res["status"],
            "fonte": "sbcp",
            "raw": payload
        }
        print(json.dumps(out, ensure_ascii=False))

    except Exception as e:
        t1 = perf_counter()
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
                "timing_ms": int((t1 - t0) * 1000),
                "tried_install": False,
                "debug": {"steps": steps[-200:]},
            },
        }
        print(json.dumps(out, ensure_ascii=False))
        # mesmo em erro retornamos JSON; o worker decide retry/failed

if __name__ == "__main__":
    main()
