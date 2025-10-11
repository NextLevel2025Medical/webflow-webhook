#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consulta_medicos.py
- Busca no site da SBCP pelo nome informado
- Abre o primeiro resultado ("Perfil Completo")
- Extrai dados básicos (nome, cidade, CRM, RQE, etc.)
- Imprime um JSON com { ok, qtd, resultados, steps, timing_ms, nome_busca, email }
- Hotfix: se o Chromium do Playwright não estiver instalado no pod, roda
  `python -m playwright install chromium` uma vez e tenta o launch novamente.
"""

import sys
import json
import time
import traceback
import subprocess

from typing import List, Dict, Any

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SBCP_URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"
MISSING_MSG = "Executable doesn't exist"


def _ensure_browsers_installed(steps: List[str]) -> bool:
    """Tenta instalar o Chromium do Playwright em runtime (fallback).
    Retorna True se instalou com sucesso."""
    try:
        steps.append("playwright install chromium (fallback em runtime)")
        proc = subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        steps.append(f"install rc={proc.returncode}")
        if proc.stdout:
            steps.append(f"install stdout: {proc.stdout[-300:]}")
        if proc.stderr:
            steps.append(f"install stderr: {proc.stderr[-300:]}")
        return proc.returncode == 0
    except Exception as e:
        steps.append(f"install exception: {e}")
        return False


def _fill_by_many_selectors(page, steps: List[str], selectors: List[str], value: str) -> bool:
    """Tenta localizar um input usando uma lista de seletores e preencher com value."""
    for sel in selectors:
        try:
            locator = page.locator(sel)
            locator.wait_for(timeout=5000)
            locator.fill("")
            locator.fill(value)
            steps.append(f'preencheu nome em "{sel}": "{value}"')
            return True
        except Exception:
            continue
    steps.append("input de nome não encontrado por nenhum seletor candidato")
    return False


def _click_by_many_selectors(page, steps: List[str], selectors: List[str]) -> bool:
    """Tenta clicar usando uma lista de seletores."""
    for sel in selectors:
        try:
            btn = page.locator(sel)
            btn.wait_for(timeout=5000)
            btn.click()
            steps.append(f'clicou no botão de busca via "{sel}"')
            return True
        except Exception:
            continue
    steps.append("botão de busca não encontrado por nenhum seletor candidato")
    return False


def _extract_profile(page, steps: List[str]) -> Dict[str, Any]:
    """Extrai campos do modal de perfil completo (estrutura dt/dd)."""
    data = {}

    try:
        modal = page.locator("div.mfp-content")
        modal.wait_for(timeout=10000)
        steps.append("modal de perfil aberto")

        # Bloco com as informações (dt/dd)
        info = modal.locator("div.cirurgiao-info")
        info.wait_for(timeout=8000)

        dts = info.locator("dt")
        dds = info.locator("dd")
        count = min(dts.count(), dds.count())

        for i in range(count):
            key = dts.nth(i).inner_text().strip().strip(":")
            val = dds.nth(i).inner_text().strip()
            if key:
                data[key] = val

        # Alguns campos úteis para padronizar
        # CRM principal
        for k in ["CRM", "Crm", "crm"]:
            if k in data and data.get("crm_padrao") is None:
                data["crm_padrao"] = data[k]
                break

        # RQE
        for k in ["RQE", "Rqe", "rqe"]:
            if k in data and data.get("rqe_padrao") is None:
                data["rqe_padrao"] = data[k]
                break

        steps.append(f"campos extraídos: {list(data.keys())}")

    except PWTimeout:
        steps.append("timeout abrindo/extraindo modal de perfil")
    except Exception as e:
        steps.append(f"erro extraindo perfil: {e}")

    return data


def buscar_sbcp(nome_busca: str, steps: List[str]) -> Dict[str, Any]:
    """Fluxo de busca no site da SBCP com hotfix de instalação do Chromium."""
    start = time.time()
    resultados = []
    tried_install = False

    with sync_playwright() as p:
        browser = None

        # Hotfix: tentar abrir; se faltar o binário, instalar e tentar de novo.
        while True:
            try:
                steps.append("launch chromium")
                browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
                break
            except Exception as e:
                msg = str(e)
                steps.append(f"launch falhou: {msg}")
                if (MISSING_MSG in msg) and (not tried_install):
                    tried_install = True
                    if _ensure_browsers_installed(steps):
                        steps.append("tentando launch novamente após install")
                        continue
                raise  # não é caso de instalação faltando, propaga

        context = browser.new_context()
        page = context.new_page()

        try:
            steps.append(f"abrindo {SBCP_URL}")
            page.goto(SBCP_URL, wait_until="commit", timeout=30000)

            # Preenche campo "Nome"
            ok_input = _fill_by_many_selectors(
                page,
                steps,
                selectors=[
                    "input#cirurgiao_nome",
                    "input[name='cirurgiao_nome']",
                    "input[placeholder*='Nome']",
                    "input[type='text']",
                ],
                value=nome_busca,
            )
            if not ok_input:
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "input_nome_nao_encontrado",
                }

            # Clica em "Buscar"
            ok_click = _click_by_many_selectors(
                page,
                steps,
                selectors=[
                    "input#cirurgiao_submit",
                    "input[name='cirurgiao_submit']",
                    "input[type='submit'][value*='Buscar']",
                    "button:has-text('Buscar')",
                    "text=Buscar",
                ],
            )
            if not ok_click:
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "botao_buscar_nao_encontrado",
                }

            # Espera aparecer algum resultado
            try:
                page.wait_for_selector(".cirurgiao-results", timeout=15000)
                steps.append("lista de resultados visível")
            except PWTimeout:
                # alguns temas não têm esse wrapper; tenta diretamente pelo item
                steps.append("wrapper de resultados não visível; tentando pelo item")
            # Confere se existe item
            itens = page.locator(".cirurgiao-results-item, .div.cirurgiao-results-item, .cirurgiao-results .row, .cirurgiao-results")
            # fallback: procurar link Perfil Completo
            perfil_link = page.locator(".cirurgiao-perfil-link, a:has-text('Perfil Completo')")
            if perfil_link.count() == 0:
                # Talvez o nome já apareça como card único; tenta achar o card pelo título
                title = page.locator("h3.cirurgiao-nome, h3:has-text('Perfil Completo')").first
                if title:
                    steps.append("nenhum link direto; resultados podem estar em outro contêiner")

            # Abre o primeiro perfil
            perfil_link = page.locator(".cirurgiao-perfil-link, a:has-text('Perfil Completo')").first
            perfil_link.click(timeout=15000)
            steps.append("clicou em Perfil Completo (primeiro resultado)")

            # Extrai as informações do modal
            dados = _extract_profile(page, steps)

            # Nome e cidade podem estar fora do bloco dt/dd; captura por seletores extras
            try:
                titulo = page.locator("div.mfp-content h3.cirurgiao-nome").first.inner_text().strip()
                if titulo:
                    dados.setdefault("nome", titulo)
            except Exception:
                pass

            # Cidade geralmente aparece no bloco “Cidade:”
            # Já foi mapeado por _extract_profile em data["Cidade"] se existir.

            if dados:
                resultados.append(dados)

            ok = len(resultados) > 0
            return {
                "ok": ok,
                "qtd": len(resultados),
                "resultados": resultados,
                "steps": steps,
                "nome_busca": nome_busca,
                "timing_ms": int((time.time() - start) * 1000),
            }

        except Exception as e:
            steps.append(f"exception: {e}")
            steps.append(traceback.format_exc()[-800:])
            return {
                "ok": False,
                "qtd": 0,
                "resultados": [],
                "steps": steps,
                "nome_busca": nome_busca,
                "timing_ms": int((time.time() - start) * 1000),
            }
        finally:
            try:
                context.close()
                browser.close()
            except Exception:
                pass


def main():
    # argv esperados:
    #   1: member_id (não usado aqui, só para o worker rastrear)
    #   2: nome
    #   3: doc (pode vir vazio)
    #   4: email
    #   5: timestamp ISO (não usado aqui)
    member_id = None
    nome = ""
    doc = ""
    email = ""
    steps: List[str] = []

    try:
        if len(sys.argv) >= 2:
            member_id = sys.argv[1]
        if len(sys.argv) >= 3:
            nome = sys.argv[2]
        if len(sys.argv) >= 4:
            doc = sys.argv[3]
        if len(sys.argv) >= 5:
            email = sys.argv[4]
    except Exception:
        pass

    steps.append(f"argv member_id={member_id} nome={nome} email={email}")

    result = buscar_sbcp(nome_busca=nome, steps=steps)
    # agrega email ao payload para o worker ter mais contexto
    result["email"] = email

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
