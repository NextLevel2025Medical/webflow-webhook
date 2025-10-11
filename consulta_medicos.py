# consulta_medicos.py — v2 robusta (Playwright, sem JSON/selenium)
import os
import sys
from typing import Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

def buscar_sbcp(nome: str, email: str = "") -> Dict:
    """
    Estratégia:
      1) Abrir página e buscar por nome.
      2) Se houver QUALQUER "Perfil Completo" na listagem => status = "ok" (não precisa abrir perfil).
      3) Se não houver, retorna "not_found".
      4) Qualquer exceção => "error".
    """
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(viewport={"width": 1366, "height": 900})
            page = ctx.new_page()
            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=30000)

                # Campo de nome (tente variações de seletor)
                nome_input = page.locator("input[name='nome'], #cirurgiao_nome, input#nome").first
                if not nome_input.count():
                    return {"status": "error", "fonte": "sbcp",
                            "reason": "campo_nome_nao_encontrado",
                            "raw": {"nome_busca": nome, "email": email, "qtd": 0, "resultados": []}}

                nome_input.fill(nome)

                # Botão Buscar (variações)
                btn_buscar = page.locator("text=Buscar, #cirurgiao_submit, button[type='submit']").first
                btn_buscar.click()

                # aguarda carregamento básico
                page.wait_for_timeout(1200)

                # espera aparecer algum resultado "Perfil Completo" (se houver)
                # não falha se não aparecer — vamos checar a contagem depois
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    pass

                qtd = page.locator("text=/Perfil Completo/i").count()

                if qtd > 0:
                    # curto-circuito: consideramos "ok" se existir qualquer perfil completo listado
                    return {
                        "status": "ok",
                        "fonte": "sbcp",
                        "raw": {"nome_busca": nome, "email": email, "qtd": qtd, "resultados": []}
                    }

                # Sem “Perfil Completo” visível → não encontrou
                return {
                    "status": "not_found",
                    "fonte": "sbcp",
                    "raw": {"nome_busca": nome, "email": email, "qtd": 0, "resultados": []}
                }

            finally:
                try:
                    ctx.close(); browser.close()
                except Exception:
                    pass

    except Exception as e:
        return {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {"nome_busca": nome, "email": email, "qtd": 0, "resultados": []}
        }

# CLI rápido para teste local:
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python consulta_medicos.py \"Nome\" [email]"); sys.exit(1)
    nome_cli = sys.argv[1]
    email_cli = sys.argv[2] if len(sys.argv) > 2 else ""
    import json
    print(json.dumps(buscar_sbcp(nome_cli, email_cli), ensure_ascii=False))
