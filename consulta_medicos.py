# consulta_medicos.py
# Playwright headless + fallback de instalação do Chromium em runtime.
# NÃO grava no banco — apenas imprime JSON. O worker consome esse JSON.
#
# Uso local:
#   python consulta_medicos.py 1364 "GUSTAVO AQUINO" "" "drgustavoaquino@yahoo.com.br" "2025-10-11T00:00:00Z"
#
# Saída (exemplo):
# {
#   "status": "ok" | "not_found" | "error",
#   "fonte": "sbcp",
#   "reason": "...(se houver)...",
#   "raw": {
#       "member_id": 1364,
#       "nome_busca": "GUSTAVO AQUINO",
#       "email": "drgustavoaquino@yahoo.com.br",
#       "qtd": 1,
#       "resultados": [],
#       "timing_ms": 2310,
#       "tried_install": true,
#       "debug": { "steps": ["...", "..."] }
#   }
# }

import os
import sys
import json
import subprocess
from time import perf_counter
from typing import Dict, List
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
def buscar_sbcp(nome: str, email: str, steps: List[str]) -> Dict:
    """
    1) Tenta abrir o Chromium. Se faltar binário, instala e tenta novamente (1x).
    2) Abre a página, preenche o nome, clica em 'Buscar'.
    3) Considera 'ok' se aparecer ao menos um 'Perfil Completo' na listagem.
    """
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

                # Botão Buscar (variações comuns)
                _log(steps, "clicando em Buscar…")
                btn_buscar = page.locator("text=Buscar, #cirurgiao_submit, button[type='submit']").first
                if not btn_buscar.count():
                    _log(steps, "ERRO: botão Buscar não encontrado")
                    return {"status": "error", "reason": "botao_buscar_nao_encontrado", "qtd": 0}

                btn_buscar.click()

                # pequeno aguardo para a listagem aparecida
                page.wait_for_timeout(1200)

                _log(steps, "aguardando resultados (Perfil Completo)…")
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    _log(steps, "timeout esperando 'Perfil Completo'")

                qtd = page.locator("text=/Perfil Completo/i").count()
                _log(steps, f"qtd_perfil_completo={qtd}")

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
        raise  # outros erros sobem


# ---------------- main/CLI ----------------
def main():
    # Aceita os 5 args que o worker envia, mas só usa member_id, nome, email
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
            "resultados": [],                # não expandimos cartões/links; suficiente p/ validação
            "timing_ms": int((t1 - t0) * 1000),
            "tried_install": bool(res.get("tried_install", False)),
            "debug": {"steps": steps[-150:]},  # limita o tamanho do log salvo no DB
        }

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
                "debug": {"steps": steps[-150:]},
            },
        }
        print(json.dumps(out, ensure_ascii=False))
        # retorna código 0 mesmo em erro, para o worker sempre capturar o JSON
        # (o controle de retry/failed acontece no worker)

if __name__ == "__main__":
    main()
