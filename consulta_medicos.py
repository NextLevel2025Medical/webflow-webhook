# consulta_medicos.py — v3 com auto-heal e logging detalhado
import os
import sys
import subprocess
from typing import Dict, List
from time import perf_counter
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

def _log(steps: List[str], msg: str):
    steps.append(msg)

def _ensure_browser_once(steps: list) -> bool:
    """
    Faz o fallback de instalação do Chromium em runtime.
    """
    try:
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
        steps.append("playwright install chromium (fallback runtime)…")
        out = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            capture_output=True, text=True, timeout=240
        )
        steps.append(f"playwright install rc={out.returncode}")
        if out.stdout:
            steps.append(f"install stdout: {out.stdout[-500:]}")
        if out.stderr:
            steps.append(f"install stderr: {out.stderr[-500:]}")
        return out.returncode == 0
    except Exception as e:
        steps.append(f"playwright install falhou: {e}")
        return False

def buscar_sbcp(nome: str, email: str = "") -> Dict:
    """
    Estratégia:
      1) Tenta lançar o Chromium. Se faltar binário, instala e tenta de novo (1x).
      2) Abre a página, preenche nome e clica em Buscar.
      3) Procura por qualquer elemento com texto 'Perfil Completo'.
      4) status = ok se qtd>0; not_found se qtd=0; error em exceção.
      5) Retorna debug.steps e métricas para auditoria detalhada.
    """
    steps: List[str] = []
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
    t0 = perf_counter()
    tried_install = False

    def _do() -> Dict:
        with sync_playwright() as pw:
            _log(steps, "launch chromium…")
            browser = pw.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(viewport={"width": 1366, "height": 900})
            page = ctx.new_page()
            try:
                _log(steps, f"goto {URL}")
                page.goto(URL, wait_until="domcontentloaded", timeout=30000)

                # Campo de nome (tente variações)
                _log(steps, "localizando campo de nome…")
                nome_input = page.locator("input[name='nome'], #cirurgiao_nome, input#nome").first
                if not nome_input.count():
                    _log(steps, "ERRO: campo de nome não encontrado.")
                    return {"status": "error", "reason": "campo_nome_nao_encontrado", "qtd": 0}

                _log(steps, f"preenchendo nome: {nome}")
                nome_input.fill(nome)

                # Botão Buscar
                _log(steps, "clicando em Buscar…")
                btn_buscar = page.locator("text=Buscar, #cirurgiao_submit, button[type='submit']").first
                if not btn_buscar.count():
                    _log(steps, "ERRO: botão Buscar não encontrado.")
                    return {"status": "error", "reason": "botao_buscar_nao_encontrado", "qtd": 0}

                btn_buscar.click()

                # Aguarda algum carregamento
                page.wait_for_timeout(1200)
                _log(steps, "aguardando resultados (Perfil Completo)…")
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    _log(steps, "timeout esperando 'Perfil Completo' — pode ser not_found.")

                qtd = page.locator("text=/Perfil Completo/i").count()
                _log(steps, f"qtd_perfil_completo={qtd}")

                status = "ok" if qtd > 0 else "not_found"
                return {"status": status, "qtd": qtd}

            finally:
                try:
                    ctx.close(); browser.close()
                except Exception:
                    pass

    try:
        try:
            res = _do()
        except Exception as e:
            msg = str(e)
            _log(steps, f"launch/navegação falhou: {msg}")
            if "Executable doesn't exist" in msg or "playwright install" in msg:
                # auto-heal: instala e tenta de novo (apenas 1x)
                tried_install = _ensure_browser_once(steps)
                res = _do()
            else:
                raise

        t1 = perf_counter()
        payload = {
            "nome_busca": nome,
            "email": email,
            "qtd": res.get("qtd", 0),
            "resultados": [],
            "timing_ms": int((t1 - t0) * 1000),
            "debug": {"steps": steps[-80:]}  # limita histórico
        }
        return {"status": res["status"], "fonte": "sbcp", "raw": payload}

    except Exception as e:
        t1 = perf_counter()
        return {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {
                "nome_busca": nome,
                "email": email,
                "qtd": 0,
                "resultados": [],
                "timing_ms": int((t1 - t0) * 1000),
                "tried_install": tried_install,
                "debug": {"steps": steps[-80:]}
            },
        }

# CLI
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python consulta_medicos.py \"Nome\" [email]"); sys.exit(1)
    nome_cli = sys.argv[1]
    email_cli = sys.argv[2] if len(sys.argv) > 2 else ""
    import json
    print(json.dumps(buscar_sbcp(nome_cli, email_cli), ensure_ascii=False))

