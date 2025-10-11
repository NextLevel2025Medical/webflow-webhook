# consulta_medicos.py
# Playwright (Chromium headless) – sem Chromedriver e sem arquivos temporários.
# Exponibiliza: buscar_sbcp(nome: str, email: str = "") -> dict
#
# Retorno:
#   {"status": "ok" | "not_found" | "error",
#    "fonte": "sbcp",
#    "reason": <str opcional>,
#    "raw": {"nome_busca": <str>, "email": <str>, "resultados": [ {...} ]}}
#
# CLI opcional para testes locais:
#   python consulta_medicos.py "Nome Sobrenome" "email@exemplo.com"

import os
import sys
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# URL oficial da busca
URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"


# ----------------------- util de extração -----------------------
def _extrair_resultados_pagina(page) -> List[dict]:
    """
    Caminha pela listagem, abre cada perfil (modal), extrai dados e fecha o modal.
    Retorna uma lista de dicts com campos relevantes.
    """
    resultados: List[dict] = []

    links = page.locator(".cirurgiao-perfil-link")
    total = links.count()
    if total == 0:
        return resultados

    for i in range(total):
        # relocaliza a cada iteração, pq ao fechar modal o DOM pode mudar
        item = page.locator(".cirurgiao-perfil-link").nth(i)
        try:
            item.click()
            page.locator(".cirurgiao-nome").wait_for(timeout=8000)

            nome_site = page.locator(".cirurgiao-nome").inner_text().strip()

            dts = page.locator(".cirurgiao-info dt")
            dds = page.locator(".cirurgiao-info dd")

            info: Dict[str, str] = {}
            n = min(dts.count(), dds.count())
            for j in range(n):
                chave = dts.nth(j).inner_text().replace(":", "").strip()
                valor = dds.nth(j).inner_text().strip()
                info[chave] = valor

            resultados.append({
                "nome_site": nome_site,
                "email": info.get("Email", ""),
                "crm": info.get("CRM", ""),
                "categoria": info.get("Categoria", "")
            })

            # fecha modal
            page.locator(".mfp-close").click()
            page.wait_for_timeout(300)
        except PWTimeout:
            # tenta fechar modal, se estiver aberto, e segue
            try:
                page.locator(".mfp-close").click()
            except Exception:
                pass
            continue
        except Exception:
            try:
                page.locator(".mfp-close").click()
            except Exception:
                pass
            continue

    return resultados


# ----------------------- API pública -----------------------
def buscar_sbcp(nome: str, email: str = "") -> Dict:
    """
    Executa a busca no portal da SBCP e retorna um dict com status + resultados.
    Usa Chromium headless via Playwright. Não grava nada em disco.
    """
    # default: headless on; defina PLAYWRIGHT_HEADLESS=0 para visualizar (local)
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"

    try:
        with sync_playwright() as play:
            browser = play.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(viewport={"width": 1280, "height": 900})
            page = ctx.new_page()

            try:
                # abre página e preenche busca
                page.goto(URL, wait_until="domcontentloaded", timeout=20000)
                page.locator("#cirurgiao_nome").fill(nome)
                page.locator("#cirurgiao_submit").click()

                # dá tempo para animações/carregamento
                page.wait_for_timeout(1500)
                # espera aparecer pelo menos um item (se houver)
                try:
                    page.locator(".cirurgiao-perfil-link").first.wait_for(timeout=7000)
                except PWTimeout:
                    pass  # sem resultados visíveis – trataremos abaixo

                resultados = _extrair_resultados_pagina(page)
                status = "ok" if resultados else "not_found"

                return {
                    "status": status,
                    "fonte": "sbcp",
                    "raw": {
                        "nome_busca": nome,
                        "email": email,
                        "resultados": resultados
                    }
                }
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass

    except Exception as e:
        # erros de rede, mudança de layout, etc.
        return {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {"nome_busca": nome, "email": email, "resultados": []}
        }


# ----------------------- CLI opcional -----------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python consulta_medicos.py \"Nome Sobrenome\" [email]")
        sys.exit(1)

    nome_cli = sys.argv[1]
    email_cli = sys.argv[2] if len(sys.argv) > 2 else ""

    import json
    print(json.dumps(buscar_sbcp(nome_cli, email_cli), ensure_ascii=False))
