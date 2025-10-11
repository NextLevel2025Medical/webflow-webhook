#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import traceback
import subprocess
from typing import List, Dict, Any

# --- DB (compat com worker_validation.py) ---
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- Playwright ---
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SBCP_URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"
MISSING_MSG = "Executable doesn't exist"

# =========================
# Helpers de BANCO (SHIMS)
# =========================

_engine_cache: Engine | None = None

def _get_engine() -> Engine:
    global _engine_cache
    if _engine_cache is None:
        db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL não configurada no ambiente.")
        _engine_cache = create_engine(db_url, pool_pre_ping=True, future=True)
    return _engine_cache

def log_validation(member_id: int, fonte: str, status: str, payload: Dict[str, Any] | None) -> None:
    """
    Insere log na tabela validations_log.
    payload é convertido para jsonb via cast explícito.
    """
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO validations_log (member_id, fonte, status, payload)
                VALUES (:member_id, :fonte, :status, CAST(:payload AS jsonb))
            """),
            {
                "member_id": int(member_id),
                "fonte": fonte,
                "status": status,
                "payload": json.dumps(payload or {}, ensure_ascii=False),
            }
        )

def set_member_validation(member_id: int, status: str, fonte: str, last_error: str | None = None) -> None:
    """
    Atualiza membersnextlevel com status de validação e portal.
    Campo last_error é opcional (se existir na sua tabela).
    """
    engine = _get_engine()
    params = {"member_id": int(member_id), "status": status, "fonte": fonte, "last_error": last_error}
    # Tenta incluir last_error se a coluna existir; se não existir, faz update sem ela.
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE membersnextlevel
                       SET validacao_acesso = :status,
                           portal_validado  = :fonte,
                           last_error       = :last_error
                     WHERE id = :member_id
                """),
                params
            )
    except Exception:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE membersnextlevel
                       SET validacao_acesso = :status,
                           portal_validado  = :fonte
                     WHERE id = :member_id
                """),
                params
            )

# =========================
# Playwright / Scraping
# =========================

def _ensure_browsers_installed(steps: List[str]) -> bool:
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
    data: Dict[str, Any] = {}
    try:
        modal = page.locator("div.mfp-content")
        modal.wait_for(timeout=10000)
        steps.append("modal de perfil aberto")

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

        # Nome do topo (h3)
        try:
            titulo = modal.locator("h3.cirurgiao-nome").first.inner_text().strip()
            if titulo:
                data.setdefault("nome", titulo)
        except Exception:
            pass

        # Normalizações úteis
        for k in ("CRM", "Crm", "crm"):
            if k in data and "crm_padrao" not in data:
                data["crm_padrao"] = data[k]
                break
        for k in ("RQE", "Rqe", "rqe"):
            if k in data and "rqe_padrao" not in data:
                data["rqe_padrao"] = data[k]
                break

        steps.append(f"campos extraídos: {list(data.keys())}")
    except PWTimeout:
        steps.append("timeout abrindo/extraindo modal de perfil")
    except Exception as e:
        steps.append(f"erro extraindo perfil: {e}")
    return data

def buscar_sbcp(nome_busca: str, steps: List[str]) -> Dict[str, Any]:
    start = time.time()
    resultados: List[Dict[str, Any]] = []
    tried_install = False

    with sync_playwright() as p:
        browser = None
        # Launch com hotfix (instala chromium se necessário)
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
                raise

        context = browser.new_context()
        page = context.new_page()
        try:
            steps.append(f"abrindo {SBCP_URL}")
            page.goto(SBCP_URL, wait_until="commit", timeout=30000)

            ok_input = _fill_by_many_selectors(
                page, steps,
                selectors=[
                    "input#cirurgiao_nome",
                    "input[name='cirurgiao_nome']",
                    "input[placeholder*='Nome']",
                    "input[type='text']",
                ],
                value=nome_busca,
            )
            if not ok_input:
                return {"ok": False, "qtd": 0, "resultados": [], "steps": steps,
                        "nome_busca": nome_busca, "timing_ms": int((time.time()-start)*1000),
                        "reason": "input_nome_nao_encontrado"}

            ok_click = _click_by_many_selectors(
                page, steps,
                selectors=[
                    "input#cirurgiao_submit",
                    "input[name='cirurgiao_submit']",
                    "input[type='submit'][value*='Buscar']",
                    "button:has-text('Buscar')",
                    "text=Buscar",
                ],
            )
            if not ok_click:
                return {"ok": False, "qtd": 0, "resultados": [], "steps": steps,
                        "nome_busca": nome_busca, "timing_ms": int((time.time()-start)*1000),
                        "reason": "botao_buscar_nao_encontrado"}

            # Espera por resultados e clica no primeiro "Perfil Completo"
            try:
                page.wait_for_selector(".cirurgiao-results", timeout=15000)
                steps.append("lista de resultados visível")
            except PWTimeout:
                steps.append("wrapper de resultados não visível; prosseguindo")

            perfil_link = page.locator(".cirurgiao-perfil-link, a:has-text('Perfil Completo')").first
            perfil_link.wait_for(timeout=15000)
            perfil_link.click()
            steps.append("clicou em Perfil Completo (primeiro resultado)")

            dados = _extract_profile(page, steps)
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

# =========================
# CLI
# =========================

def main():
    # argv: [member_id, nome, doc, email, ts]
    member_id = None
    nome = ""
    doc = ""
    email = ""
    try:
        if len(sys.argv) >= 2: member_id = sys.argv[1]
        if len(sys.argv) >= 3: nome = sys.argv[2]
        if len(sys.argv) >= 4: doc = sys.argv[3]
        if len(sys.argv) >= 5: email = sys.argv[4]
    except Exception:
        pass

    steps: List[str] = [f"argv member_id={member_id} nome={nome} email={email}"]
    result = buscar_sbcp(nome_busca=nome, steps=steps)
    result["email"] = email
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()
