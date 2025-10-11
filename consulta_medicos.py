#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
consulta_medicos.py (versão tolerante)

- Wrapper buscar_sbcp(*args, **kwargs) aceita múltiplas assinaturas:
    (nome_busca, steps)
    (conn, nome_busca, steps)
    (conn, member_id, nome_busca, steps)
  e também via kwargs (nome/nome_busca, steps).

- _buscar_sbcp_core(nome_busca, steps): implementação real (Playwright + scraping).

- log_validation / set_member_validation: tolerantes a conexão como 1º arg, args extras
  e kwargs; fazem CAST para jsonb.

- Hotfix: instala Chromium do Playwright em runtime se necessário.
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
    global _engine_cache
    if _engine_cache is None:
        db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
        if not db_url:
            raise RuntimeError("DATABASE_URL não configurada no ambiente.")
        _engine_cache = create_engine(db_url, pool_pre_ping=True, future=True)
    return _engine_cache

def _looks_like_connection(obj) -> bool:
    try:
        name = obj.__class__.__name__.lower()
        mod  = obj.__class__.__module__.lower()
        return ("connection" in name) or ("psycopg2" in mod) or ("asyncpg" in mod)
    except Exception:
        return False

def log_validation(*args, **kwargs) -> None:
    """
    Aceita:
      log_validation(member_id, fonte, status, payload, ...)
      log_validation(conn, member_id, fonte, status, payload, ...)
      também via kwargs. Extras vão para payload["extra"].
    """
    engine = _get_engine()
    args = list(args)
    if args and _looks_like_connection(args[0]):
        args.pop(0)

    member_id = args[0] if len(args) > 0 else kwargs.pop("member_id", None)
    fonte     = args[1] if len(args) > 1 else kwargs.pop("fonte", None)
    status    = args[2] if len(args) > 2 else kwargs.pop("status", None)
    payload   = args[3] if len(args) > 3 else kwargs.pop("payload", None)

    if payload is None:
        payload = {}
    elif not isinstance(payload, dict):
        try:
            payload = dict(payload)
        except Exception:
            payload = {"value": str(payload)}

    extra: Dict[str, Any] = {}
    if len(args) > 4:
        extra["args"] = [repr(a) for a in args[4:]]
    if kwargs:
        extra["kwargs"] = kwargs
    if extra:
        payload["extra"] = extra

    try:
        member_id_int = int(member_id) if member_id is not None else None
    except Exception:
        payload.setdefault("extra", {})
        payload["extra"]["member_id_parse_error"] = repr(member_id)
        member_id_int = None

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
    Aceita:
      set_member_validation(member_id, status, fonte, last_error=None)
      set_member_validation(conn, member_id, status, fonte, last_error=None)
      também via kwargs.
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
        return

    params = {"member_id": member_id_int, "status": status, "fonte": fonte, "last_error": last_error}

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

        try:
            titulo = modal.locator("h3.cirurgiao-nome").first.inner_text().strip()
            if titulo:
                data.setdefault("nome", titulo)
        except Exception:
            pass

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

def _buscar_sbcp_core(nome_busca: str, steps: List[str]) -> Dict[str, Any]:
    """Implementação real da busca (antes chamada buscar_sbcp)."""
    start = time.time()
    resultados: List[Dict[str, Any]] = []
    tried_install = False

    with sync_playwright() as p:
        browser = None
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

# ------------ WRAPPER TOLERANTE ------------
def buscar_sbcp(*args, **kwargs) -> Dict[str, Any]:
    """
    Aceita várias assinaturas:
      - buscar_sbcp(nome_busca, steps)
      - buscar_sbcp(conn, nome_busca, steps)
      - buscar_sbcp(conn, member_id, nome_busca, steps)
      - buscar_sbcp(nome=..., nome_busca=..., steps=[...])

    Retorna o dicionário produzido por _buscar_sbcp_core.
    """
    args = list(args)

    # 1) descarta conexão no início
    if args and _looks_like_connection(args[0]):
        args.pop(0)

    nome_busca = None
    steps = None

    # 2) tenta detectar padrão (member_id) como segundo arg (int/str-num)
    def _is_intlike(x):
        try:
            int(str(x))
            return True
        except Exception:
            return False

    # Casos por posição
    if len(args) == 1:
        # (nome_busca)
        nome_busca = args[0]
    elif len(args) == 2:
        # (nome_busca, steps) ou (member_id, nome_busca)
        if _is_intlike(args[0]) and not _is_intlike(args[1]):
            # (member_id, nome)
            nome_busca = args[1]
        else:
            nome_busca = args[0]
            steps = args[1]
    elif len(args) >= 3:
        # (member_id?, nome_busca, steps, ...)
        if _is_intlike(args[0]):
            nome_busca = args[1]
            steps = args[2]
        else:
            nome_busca = args[0]
            steps = args[1] if isinstance(args[1], list) else args[2] if len(args) > 2 else None

    # 3) sobrescreve por kwargs se vierem
    if "nome_busca" in kwargs and kwargs["nome_busca"]:
        nome_busca = kwargs["nome_busca"]
    if "nome" in kwargs and kwargs["nome"]:
        nome_busca = kwargs["nome"]
    if "steps" in kwargs and isinstance(kwargs["steps"], list):
        steps = kwargs["steps"]

    if steps is None:
        steps = []
    if nome_busca is None:
        steps.append("wrapper: nome_busca ausente")
        return {"ok": False, "qtd": 0, "resultados": [], "steps": steps, "reason": "nome_busca_ausente"}

    steps.append(f"wrapper normalizado: nome_busca='{nome_busca}'")
    return _buscar_sbcp_core(nome_busca=nome_busca, steps=steps)

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
