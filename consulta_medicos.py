# consulta_medicos.py — versão robusta (Playwright, sem JSON)
import os
import sys
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

def _normaliza(s: str) -> str:
    return (s or "").replace(":", "").strip()

def _extrai_info_por_dt_dd(page: Page) -> Dict[str, str]:
    """Lê pares dt/dd se existirem no layout atual."""
    info = {}
    dts = page.locator("dt")
    dds = page.locator("dd")
    n = min(dts.count(), dds.count())
    for i in range(n):
        k = _normaliza(dts.nth(i).inner_text())
        v = _normaliza(dds.nth(i).inner_text())
        if k:
            info[k] = v
    return info

def _tenta_extrair_no_modal(page: Page) -> Dict[str, str]:
    """
    Alguns layouts abrem modal. Tenta extrair e fecha o modal se presente.
    Retorna dict vazio se não houver modal aberto.
    """
    try:
        # seletor usado em versões antigas
        page.locator(".cirurgiao-nome").wait_for(timeout=2500)
        nome_site = _normaliza(page.locator(".cirurgiao-nome").inner_text())
        info = _extrai_info_por_dt_dd(page.locator(".cirurgiao-info").first if page.locator(".cirurgiao-info").count() else page)
        try:
            page.locator(".mfp-close, .modal .close, .mfp-wrap .mfp-close").first.click()
        except Exception:
            pass
        return {
            "nome_site": nome_site,
            "email": info.get("Email", ""),
            "crm": info.get("CRM", "") or info.get("CRM/UF", ""),
            "categoria": info.get("Categoria", "") or info.get("Categoria/Área", "")
        }
    except PWTimeout:
        return {}

def _tenta_extrair_na_pagina_de_perfil(page: Page) -> Dict[str, str]:
    """
    Muitos resultados abrem uma NOVA PÁGINA de perfil. Tenta extrair título e informações.
    """
    nome_site = ""
    # tentativas de seletor para o nome
    for sel in ["h1", ".titulo", ".page-title", ".cirurgiao-nome"]:
        loc = page.locator(sel)
        if loc.count():
            nome_site = _normaliza(loc.first.inner_text())
            if nome_site:
                break

    # tenta ler blocos dt/dd ou listas
    info = {}
    if page.locator("dt").count() and page.locator("dd").count():
        info = _extrai_info_por_dt_dd(page)
    else:
        # alguns perfis usam listas/labels simples
        # heurística: pega pares onde o label contém e-mail/CRM/categoria
        textos = page.locator("text=/@/i, text=/CRM/i, text=/Categoria/i")
        # (só deixa para o dt/dd acima na maioria dos casos)
        pass

    return {
        "nome_site": nome_site,
        "email": info.get("Email", ""),
        "crm": info.get("CRM", "") or info.get("CRM/UF", ""),
        "categoria": info.get("Categoria", "") or info.get("Categoria/Área", "")
    }

def _extrair_resultados(page: Page) -> List[dict]:
    """
    Estratégia:
      1) encontre todos os links/elementos com texto 'Perfil Completo'
      2) para cada um: clique e tente extrair (modal OU navegação)
      3) volte (se navegou)
    """
    resultados: List[dict] = []

    # anchors ou botões com esse texto (com ou sem acento/maiúsculas)
    perfis = page.locator("a:has-text('Perfil Completo'), button:has-text('Perfil Completo')")
    total = perfis.count()
    if total == 0:
        return resultados

    for i in range(total):
        link = page.locator("a:has-text('Perfil Completo'), button:has-text('Perfil Completo')").nth(i)
        # salva URL atual para conseguir voltar depois
        url_base = page.url
        try:
            # algumas vezes abre modal, outras navega; tratamos os dois
            with page.expect_navigation(wait_until="domcontentloaded", timeout=4000) as nav:
                link.click()
            navegou = True
        except PWTimeout:
            # provavelmente abriu modal, então não navegou
            try:
                link.click()
            except Exception:
                pass
            navegou = False

        try:
            if not navegou:
                item = _tenta_extrair_no_modal(page)
            else:
                item = _tenta_extrair_na_pagina_de_perfil(page)

            # só aceita se ao menos nome foi capturado (o portal nem sempre expõe e-mail)
            if item.get("nome_site"):
                resultados.append(item)
        finally:
            # fecha modal ou volta para a listagem se tiver navegado
            if navegou:
                try:
                    page.go_back(wait_until="domcontentloaded", timeout=6000)
                except Exception:
                    # última tentativa: retorna para URL base
                    try:
                        page.goto(url_base, wait_until="domcontentloaded", timeout=6000)
                    except Exception:
                        pass
            else:
                try:
                    page.locator(".mfp-close, .modal .close, .mfp-wrap .mfp-close").first.click()
                except Exception:
                    pass
            page.wait_for_timeout(300)

    return resultados

def buscar_sbcp(nome: str, email: str = "") -> Dict:
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(viewport={"width": 1366, "height": 900})
            page = ctx.new_page()

            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=25000)
                # preenche só o nome; filtros (categoria/estado) deixam como 'Todos'
                page.locator("input[name='nome'], #cirurgiao_nome, input#nome").first.fill(nome)
                page.locator("text=Buscar, #cirurgiao_submit, button[type='submit']").first.click()

                # espera carregar algo (ou nada)
                page.wait_for_timeout(1200)
                # se existir um grid de resultados, deixa mais tempo para aparecer o primeiro
                try:
                    page.locator("text=/Perfil Completo/i").first.wait_for(timeout=7000)
                except PWTimeout:
                    pass

                resultados = _extrair_resultados(page)
                status = "ok" if resultados else "not_found"

                return {
                    "status": status,
                    "fonte": "sbcp",
                    "raw": {"nome_busca": nome, "email": email, "qtd": len(resultados), "resultados": resultados}
                }
            finally:
                try:
                    ctx.close()
                    browser.close()
                except Exception:
                    pass
    except Exception as e:
        return {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {"nome_busca": nome, "email": email, "qtd": 0, "resultados": []}
        }

# CLI de teste local: python consulta_medicos.py "GUSTAVO AQUINO"
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("uso: python consulta_medicos.py \"Nome\" [email]"); sys.exit(1)
    nome_cli = sys.argv[1]
    email_cli = sys.argv[2] if len(sys.argv) > 2 else ""
    import json
    print(json.dumps(buscar_sbcp(nome_cli, email_cli), ensure_ascii=False))
