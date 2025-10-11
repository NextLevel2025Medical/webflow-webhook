#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consulta_medicos.py

- Abre o site da SBCP e busca pelo nome.
- Clica no primeiro "Perfil Completo" e extrai dados do modal.
- Imprime JSON em stdout com { ok, qtd, resultados, steps, timing_ms, nome_busca, email }.
- Hotfix: se o Chromium do Playwright não estiver presente no pod, roda
  `python -m playwright install chromium` e tenta novamente.

Também expõe shims de banco compatíveis com o worker:
- log_validation(...)
- set_member_validation(...)

Ambiente:
  - DATABASE_URL (ou NEON_DATABASE_URL) com string de conexão Postgres.
"""

import os
import sys
import json
import time
import traceback
import subprocess
from typing import List, Dict, Any, Optional

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

_engine_cache: Optional[Engine] = None

def _get_engine() -> Engine:
    """Cria (e cacheia) o engine SQLAlchemy a partir da env var."""
    global _engine_cache
    if _engine_cache is None:
        db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL não configurada no ambiente.")
        _engine_cache = create_engine(db_url, pool_pre_ping=True, future=True)
    return _engine_cache

def _looks_like_connection(obj) -> bool:
    """Detecta se obj parece ser uma conexão (psycopg2, etc.) passada por engano como 1º argumento."""
    try:
        name = obj.__class__.__name__.lower()
        mod  = obj.__class__.__module__.lower()
        return ("connection" in name) or ("psycopg2" in mod) or ("asyncpg" in mod)
    except Exception:
        return False

def log_validation(*args, **kwargs) -> None:
    """
    Formatos aceitos (todos válidos):
      - log_validation(member_id, fonte, status, payload)
      - log_validation(conn, member_id, fonte, status, payload)
      - log_validation(member_id, fonte, status, payload, <extra...>)
      - log_validation(..., payload=<dict>, steps=<list>, reason=<str>, ...)
    Extras vão para payload["extra"].
    """
    engine = _get_engine()
    args = list(args)

    # Se o primeiro argumento for uma conexão (psycopg2/whatever), ignoramos
    if args and _looks_like_connection(args[0]):
        args.pop(0)

    # Extrai campos principais por posição/kwargs
    member_id = args[0] if len(args) > 0 else kwargs.pop("member_id", None)
    fonte     = args[1] if len(args) > 1 else kwargs.pop("fonte", None)
    status    = args[2] if len(args) > 2 else kwargs.pop("status", None)
    payload   = args[3] if len(args) > 3 else kwargs.pop("payload", None)

    # Normaliza payload
    if payload is None:
        payload = {}
    elif not isinstance(payload, dict):
        try:
            payload = dict(payload)
        except Exception:
            payload = {"value": str(payload)}

    # Qualquer resto vai para payload["extra"]
    extra: Dict[str, Any] = {}
    if len(args) > 4:
        extra["args"] = [repr(a) for a in args[4:]]
    if kwargs:
        extra["kwargs"] = kwargs
    if extra:
        payload["extra"] = extra

    # Converte member_id com segurança
    try:
        member_id_int = int(member_id) if member_id is not None else None
    except Exception:
        payload.setdefault("extra", {})
        payload["extra"]["member_id_parse_error"] = repr(member_id)
        member_id_int = None  # não derruba o log

    payload_json = json.dumps(payload, ensure_ascii=False)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO validations_log (member_id, fonte, status, payload)
                VALUES (:member_id, :fonte, :status, CAST(:payload AS jsonb))
            """),
            {
                "member_id": member_id_int,
                "fonte": fonte,
                "status": status,
                "payload": payload_json,
            }
        )

def set_member_validation(*args, **kwargs) -> None:
    """
    Formatos aceitos:
      - set_member_validation(member_id, status, fonte, last_error=None)
      - set_member_validation(conn, member_id, status, fonte, last_error=None)
      - set_member_validation(..., member_id=, status=, fonte=, last_error=)
    Ignora 1º arg se for conexão. Se coluna last_error não existir, faz update sem ela.
    """
    engine = _get_engine()

    args = list(args)
    if args and _looks_like_connection(args[0]):
        args.pop(0)

    member_id = args[0] if len(args) > 0 else kwargs.pop("member_id", None)
    status    = args[1] if len(args) > 1 else kwargs.pop("status", None)
    fonte     = args[2] if len(args) > 2 else kwargs.pop("fonte", None)
    last_error = args[3] if len(args) > 3 else kwargs.pop("last_error", None)

    try:
        member_id_int = int(member_id) if member_id is not None else None
    except Exception:
        # não atualiza se não der para interpretar o id
        return

    params = {"member_id": member_id_int, "status": status, "fonte": fonte, "last_error": last_error}

    # tenta com last_error; se a coluna não existir, faz fallback
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
    """Fallback: baixa Chromium do Playwright em runtime."""
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

        # Nome (topo do modal)
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
    """Fluxo de busca no site da SBCP com hotfix de instalação do Chromium."""
    start = time.time()
    resultados: List[Dict[str, Any]] = []
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

            # Espera wrapper e clica no primeiro "Perfil Completo"
            try:
                page.wait_for_selector(".cirurgiao-results", timeout=15000)
                steps.append("lista de resultados visível")
            except PWTimeout:
                steps.append("wrapper de resultados não visível; prosseguindo")

            perfil_link = page.locator(".cirurgiao-perfil-link, a:has-text('Perfil Completo')").first
            perfil_link.wait_for(timeout=15000)
            perfil_link.click()
            steps.append("clicou em Perfil Completo (primeiro resultado)")

            # Extrai as informações do modal
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

    steps: List[str] = [f"argv member_id={member_id} nome={nome} email={email}"]
    result = buscar_sbcp(nome_busca=nome, steps=steps)
    result["email"] = email
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
