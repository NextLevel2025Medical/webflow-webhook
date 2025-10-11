# consulta_medicos.py
# -*- coding: utf-8 -*-

import json
import os
import re
import sys
import time
import unicodedata
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


SITE = "https://cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

HEADLESS = True  # deixe True no Render
NAV_TIMEOUT_MS = 45_000
STEP_TIMEOUT_MS = 30_000


def norm(s: str) -> str:
    """Normaliza para comparação (sem acentos/caixa)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join([c for c in s if not unicodedata.combining(c)])
    return s.upper().strip()


def try_fill_name(page, nome: str, steps: List[str]) -> None:
    """
    Tenta vários seletores possíveis para o input do nome
    (o site é WP e já mudou markup).
    """
    candidatos = [
        'input#cirurgiao_nome',
        'input[name="cirurgiao_nome"]',
        'input[name="nome"]',
        'input[placeholder*="Nome"]',
        'form#cirurgiao-form input[type="text"]',
        'form[action*="index.php"] input[type="text"]',
    ]
    for css in candidatos:
        try:
            page.locator(css).first.wait_for(timeout=STEP_TIMEOUT_MS, state="visible")
            page.locator(css).first.fill(nome)
            steps.append(f'preencheu nome em: {css}')
            return
        except PWTimeout:
            steps.append(f'input nao encontrado: {css}')
            continue
    raise RuntimeError("campo_nome_nao_encontrado")


def click_buscar(page, steps: List[str]) -> None:
    """Clica no botão Buscar (com fallback de seletores)."""
    try:
        page.locator('input#cirurgiao_submit').first.wait_for(timeout=STEP_TIMEOUT_MS, state="visible")
        page.locator('input#cirurgiao_submit').first.click()
        steps.append("clicou em: input#cirurgiao_submit")
        return
    except PWTimeout:
        steps.append("nao achou input#cirurgiao_submit, tentando por role")
        pass

    # Fallback por role/nome
    try:
        page.get_by_role("button", name=re.compile(r"Buscar", re.I)).first.click(timeout=STEP_TIMEOUT_MS)
        steps.append("clicou em: role=button name~Buscar")
        return
    except PWTimeout:
        raise RuntimeError("botao_buscar_nao_encontrado")


def wait_results(page, steps: List[str]) -> None:
    """
    Aguarda a área de resultados aparecer/atualizar.
    Reconhece tanto container quanto link 'Perfil Completo'.
    """
    # Muitas vezes a página rola a lista; garanta visibilidade
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)
    try:
        page.locator(".cirurgiao-results, .cirurgiao-results-wrapper").first.wait_for(
            timeout=NAV_TIMEOUT_MS, state="visible"
        )
        steps.append("container resultados visível")
    except PWTimeout:
        steps.append("container resultados nao visível, tentando pelo link Perfil Completo")

    # De todo modo, se não houver container, o link também serve pra sinalizar resultado
    try:
        page.locator("a.cirurgiao-perfil-link, a:has-text('Perfil Completo')").first.wait_for(
            timeout=NAV_TIMEOUT_MS, state="visible"
        )
        steps.append("link Perfil Completo visível")
    except PWTimeout:
        # pode ser lista vazia – deixamos seguir, será tratado no clique
        steps.append("link Perfil Completo nao visível (possível lista vazia)")


def click_perfil(page, nome_alvo: str, steps: List[str]) -> bool:
    """
    Clica no link Perfil Completo. Se houver múltiplos, tenta
    o que estiver dentro de um card contendo o nome alvo.
    Retorna True se clicou em algum perfil.
    """
    link_sel = "a.cirurgiao-perfil-link, a:has-text('Perfil Completo')"

    # 1) Tentar card com o nome
    try:
        card = page.locator(".cirurgiao-results-item, .cirurgiao-results, .cirurgiao-resultado, .cirurgiao-results-row") \
                  .filter(has_text=re.compile(re.escape(nome_alvo), re.I)).first
        if card.count() > 0:
            card.locator(link_sel).first.click(timeout=STEP_TIMEOUT_MS)
            steps.append("clicou Perfil Completo no card do nome")
            return True
    except PWTimeout:
        steps.append("nao conseguiu clicar no perfil do card com o nome")

    # 2) Fallback: primeiro Perfil Completo da lista
    try:
        page.locator(link_sel).first.click(timeout=STEP_TIMEOUT_MS)
        steps.append("clicou no primeiro Perfil Completo")
        return True
    except PWTimeout:
        steps.append("nenhum Perfil Completo clicável encontrado")
        return False


def extract_modal_info(page, steps: List[str]) -> Dict[str, str]:
    """
    No modal (magnific-popup), extrai os pares dt/dd
    e retorna dict com campos úteis.
    """
    # Espera o modal
    try:
        page.locator(".mfp-content").first.wait_for(timeout=STEP_TIMEOUT_MS, state="visible")
        steps.append("modal aberto")
    except PWTimeout:
        raise RuntimeError("modal_nao_abriu")

    # Mapear dt -> dd
    dts = page.locator(".mfp-content dt")
    dds = page.locator(".mfp-content dd")

    info = {}
    try:
        n = dts.count()
    except Exception:
        n = 0

    for i in range(n):
        try:
            k = dts.nth(i).inner_text().strip().rstrip(":")
            v = dds.nth(i).inner_text().strip()
            info[k] = v
        except Exception:
            # ignorar pares quebrados
            pass

    steps.append(f"coletou_campos: {list(info.keys())}")

    # Normaliza chaves de interesse
    result = {
        "cidade": info.get("Cidade", ""),
        "categoria": info.get("Categoria", ""),
        "crm": info.get("CRM", ""),
        "crm2": info.get("CRM 2", ""),
        "rqe": info.get("RQE", ""),
    }
    return result


def buscar_sbcp(nome: str, email: str, steps: List[str]) -> Dict:
    inicio = time.time()
    resultados: List[Dict] = []

    with sync_playwright() as p:
        steps.append("launch chromium…")
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--disable-features=TranslateUI",
                "--disable-extensions",
            ],
        )
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto(SITE, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
            steps.append("abriu pagina de busca")

            try_fill_name(page, nome, steps)
            click_buscar(page, steps)
            wait_results(page, steps)

            # Verifica se há resultados
            if not click_perfil(page, nome, steps):
                steps.append("nenhum perfil clicável após busca")
                return {
                    "qtd": 0,
                    "email": email,
                    "nome_busca": nome,
                    "resultados": resultados,
                }

            # Extrair dados do modal
            info = extract_modal_info(page, steps)
            resultados.append(
                {
                    "nome": nome,
                    "email": email,
                    **info,
                }
            )

            return {
                "qtd": len(resultados),
                "email": email,
                "nome_busca": nome,
                "resultados": resultados,
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


def main():
    """
    Aceita:
      argv[1] = member_id (ignorado aqui, mas mantido p/ compat)
      argv[2] = nome
      argv[3] = telefone (opcional/ignorado)
      argv[4] = email
      argv[5] = timestamp (opcional)
    Imprime JSON com:
      {
        "status": "ok"|"error",
        "fonte": "sbcp",
        "raw": { ... debug, resultados ... }
      }
    """
    steps: List[str] = []
    try:
        # Parse argumentos de forma tolerante
        argv = sys.argv
        member_id = argv[1] if len(argv) > 1 else ""
        nome = argv[2] if len(argv) > 2 else ""
        email = argv[4] if len(argv) > 4 else (argv[3] if len(argv) > 3 else "")

        if not nome:
            raise RuntimeError("param_nome_ausente")

        payload = buscar_sbcp(nome=nome, email=email, steps=steps)
        payload["debug"] = {"steps": steps}
        payload["timing_ms"] = int((time.time() - (time.time() - 0)) * 1000)  # placeholder simples

        out = {
            "status": "ok",
            "fonte": "sbcp",
            "raw": payload,
        }
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        out = {
            "status": "error",
            "fonte": "sbcp",
            "reason": str(e),
            "raw": {
                "qtd": 0,
                "resultados": [],
                "debug": {"steps": steps},
            },
        }
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
