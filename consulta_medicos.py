# -*- coding: utf-8 -*-
"""
Scraper da SBCP (cirurgiaplastica.org.br) usando Playwright (API síncrona).

Principais funções públicas:
- buscar_sbcp(member_id, nome, email, steps): executa a busca pelo nome e tenta abrir o 1º perfil; extrai todos os
  campos exibidos no perfil em pares <dt>/<dd> dentro de ".cirurgiao-info" e normaliza CRM/RQE/CREFITO.
- (opcionais) log_validation(...) e set_member_validation(...) para registrar no banco, se desejar.

Melhorias implementadas:
1) [PATCH 1] Espera explícita do container correto de perfil (".cirurgiao-info") após abrir o 1º resultado.
2) [PATCH 2] Extração via leitura de pares <dt>/<dd> dentro de ".cirurgiao-info"; só depois aplica normalização
   para gerar crm_padrao, rqe_padrao, crefito_padrao (ex.: "32019" ou "32019-BA"), preservando arrays de múltiplos.

Extras:
- Busca tolerante (múltiplos seletores e timeouts razoáveis).
- Passo-a-passo detalhado em "steps" para depuração.
- Fallback de instalação automática do Chromium (opcional) via _ensure_playwright_browsers().
"""

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

# Banco (opcional, usado só se for chamar log_validation/set_member_validation)
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # permite rodar sem Postgres instalado

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"


# =========================
# Utilitários de normalização
# =========================
def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _num_uf(s: str) -> str:
    """
    Normaliza valores como: "12345 MG", "12345/MG", "12345-MG" -> "12345-MG"
    Se não houver UF, devolve apenas o número. Mantém somente a primeira ocorrência.
    """
    s = (s or "").upper().strip()
    if not s:
        return ""
    m = re.search(r"(\d+)\s*(?:[-/ ]\s*([A-Z]{2}))?", s)
    if not m:
        return _digits(s)
    num = m.group(1)
    uf = m.group(2)
    return f"{num}-{uf}" if uf else num


def _strip_accents_lower(s: str) -> str:
    import unicodedata
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s.rstrip(":")


def _split_multi_ids(v: Optional[str]) -> List[str]:
    """
    De uma string possível com múltiplos IDs, extrai todos os “número[-/ ]UF?”,
    ex.: "CRM 41255 / MG, RQE 32019" -> ["41255 MG", "32019"]
    """
    if not v:
        return []
    found = re.findall(r"\d+\s*(?:[-/ ]\s*[A-Z]{2})?", v.upper())
    return [x.strip() for x in found] if found else [v.strip()]


# =========================
# Helpers de banco (opcionais)
# =========================
def get_conn() -> Optional["psycopg2.extensions.connection"]:
    if not psycopg2 or not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)


def log_validation(conn, member_id: int, fonte: str, status: str, payload: Dict[str, Any]) -> None:
    """
    Insere um registro em validations_log. Opcional.
    """
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO validations_log (member_id, fonte, status, payload, created_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW())
            """,
            (member_id, fonte, status, json_dumps(payload)),
        )
    conn.commit()


def set_member_validation(conn, member_id: int, status: str, fonte: str) -> None:
    """
    Atualiza membersnextlevel.status_validation e .fonte_validation. Opcional.
    """
    if not conn:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE membersnextlevel
               SET status_validation = %s,
                   fonte_validation  = %s,
                   updated_at        = NOW()
             WHERE id = %s
            """,
            (status, fonte, member_id),
        )
    conn.commit()


def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# =========================
# Playwright helpers
# =========================
def _ensure_playwright_browsers(steps: List[str]) -> None:
    """
    Opcional: tenta instalar chromium caso não esteja disponível no container.
    Útil para ambientes como Render em first-boot.
    """
    try:
        steps.append("try_playwright_install_check")
        # tentativa leve: abrir playwright; se falhar, instalar
        with sync_playwright() as p:
            _ = p.chromium
        steps.append("playwright_ok")
    except Exception as e:
        steps.append(f"playwright_missing:{e}")
        try:
            import subprocess
            subprocess.run(
                ["python", "-m", "playwright", "install", "--with-deps", "chromium"],
                check=True,
                capture_output=True,
                text=True,
            )
            steps.append("chromium_installed")
        except Exception as e2:
            steps.append(f"chromium_install_error:{e2}")


def _try_select(page, selectors: List[str], timeout: int = 10000, steps: Optional[List[str]] = None):
    """
    Tenta localizar o primeiro seletor visível dentre os candidatos.
    """
    for css in selectors:
        try:
            loc = page.locator(css).first
            loc.wait_for(state="visible", timeout=timeout)
            if steps is not None:
                steps.append(f"found_selector:{css}")
            return loc
        except Exception:
            continue
    raise PWTimeout(f"Nenhum seletor correspondente ficou visível: {selectors}")


# =========================
# EXTRAÇÃO do perfil (PATCH 2)
# =========================
def _extract_profile(page, steps: List[str]) -> Dict[str, Any]:
    """
    Extrai todos os dados do perfil lendo pares <dt>/<dd> dentro de .cirurgiao-info.
    Depois normaliza CRM/RQE/CREFITO. Mantém arrays *_padrao se houver múltiplos.
    """
    dados: Dict[str, Any] = {}

    # Nome (fora ou dentro do container)
    try:
        nome_sel = page.locator("h1, h2, .perfil-nome, .titulo-perfil").first
        nome_txt = nome_sel.inner_text(timeout=3000).strip()
        if nome_txt:
            dados["nome"] = nome_txt
            steps.append("ok_nome")
    except Exception:
        steps.append("miss_nome")

    # Container principal
    container = page.locator(".cirurgiao-info").first
    try:
        container.wait_for(state="visible", timeout=3000)
    except Exception:
        steps.append("miss_cirurgiao_info")
        return dados

    # Pares dt/dd
    try:
        dt_texts = [t.strip() for t in container.locator("dt").all_inner_texts()]
        dd_texts = [t.strip() for t in container.locator("dd").all_inner_texts()]
    except Exception as e:
        steps.append(f"miss_dt_dd:{e}")
        return dados

    n = min(len(dt_texts), len(dd_texts))
    steps.append(f"pares_dt_dd={n}")

    # Mapa bruto: chave normalizada -> valor raw
    raw_map: Dict[str, str] = {}
    for i in range(n):
        k_raw = dt_texts[i]
        v_raw = dd_texts[i]
        k = _strip_accents_lower(k_raw)
        raw_map[k] = v_raw
        # também guardar as chaves brutas para auditoria opcional
        dados.setdefault("_raw_pairs", []).append({"k": k_raw, "v": v_raw})

    # Aliases possíveis no site
    aliases = {
        "crm": ["crm", "registro crm", "crm/uf", "crm uf", "nº crm", "numero crm"],
        "rqe": [
            "rqe",
            "registro de qualificacao",
            "registro de qualificacao especialista",
            "registro de qualificação",
            "registro de qualificação especialista",
        ],
        "crefito": ["crefito", "registro crefito", "nº crefito", "numero crefito"],
    }

    def first_by_alias(keys: List[str]) -> Optional[str]:
        for k in keys:
            if k in raw_map and raw_map[k]:
                return raw_map[k]
        return None

    crm_val = first_by_alias(aliases["crm"])
    rqe_val = first_by_alias(aliases["rqe"])
    crefito_val = first_by_alias(aliases["crefito"])

    # montar listas (se houver múltiplos na string)
    crms = _split_multi_ids(crm_val)
    rqes = _split_multi_ids(rqe_val)
    crefitos = _split_multi_ids(crefito_val)

    if crms:
        dados["crm"] = crms[0]
        dados["crms"] = crms
        dados["crm_padrao"] = _num_uf(crms[0])
        dados["crms_padrao"] = [_num_uf(x) for x in crms]

    if rqes:
        dados["rqe"] = rqes[0]
        dados["rqes"] = rqes
        dados["rqe_padrao"] = _num_uf(rqes[0])
        dados["rqes_padrao"] = [_num_uf(x) for x in rqes]

    if crefitos:
        dados["crefito"] = crefitos[0]
        dados["crefitos"] = crefitos
        dados["crefito_padrao"] = _num_uf(crefitos[0])
        dados["crefitos_padrao"] = [_num_uf(x) for x in crefitos]

    # Dump parcial do HTML para debug (opcional)
    try:
        html_snip = container.inner_html(timeout=2000)[:800]
        dados["_perfil_html_snippet"] = html_snip
        steps.append("perfil_html_snippet_ok")
    except Exception:
        steps.append("perfil_html_snippet_fail")

    # Mantém também o dicionário bruto (normalizado nas chaves)
    dados["_raw_map"] = raw_map
    return dados


# =========================
# BUSCA e navegação (inclui PATCH 1)
# =========================
def buscar_sbcp(member_id: Optional[int], nome: str, email: Optional[str] = None, steps: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Abre a página de busca, pesquisa pelo nome, clica no 1º resultado e extrai o perfil.
    Retorno:
      {
        "ok": bool,
        "qtd": int,                # quantidade de resultados (se detectável)
        "resultados": list,        # opcional, se implementar varredura múltipla
        "dados": dict,             # dicionário com campos do perfil (nome, crm, rqe, etc.)
        "steps": list[str],        # trilha de auditoria
        "nome_busca": str,
        "timing_ms": int,
        "reason": "motivo"         # se ok=False
      }
    """
    if steps is None:
        steps = []
    start = time.time()
    nome_busca = (nome or "").strip()

    if not nome_busca:
        return {
            "ok": False,
            "qtd": 0,
            "resultados": [],
            "steps": steps + ["nome_vazio"],
            "nome_busca": nome_busca,
            "timing_ms": int((time.time() - start) * 1000),
            "reason": "nome_vazio",
        }

    _ensure_playwright_browsers(steps)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(ignore_https_errors=True, java_script_enabled=True)
            page = context.new_page()
            page.set_default_timeout(25000)

            # 1) Abre página
            steps.append("launch chromium")
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
            steps.append(f"abrindo {BASE_URL}")

            # 2) Preenche campo de nome (tentando vários seletores com fallback)
            nome_selectors = [
                "input#cirurgiao_nome",
                "input[name='cirurgiao_nome']",
                "input[placeholder*='Nome']",
                "input[type='text']",
            ]
            try:
                nome_input = _try_select(page, nome_selectors, timeout=15000, steps=steps)
                nome_input.fill(nome_busca)
                steps.append(f"preencheu:input#cirurgiao_nome='{nome_busca}'")
            except PWTimeout:
                steps.append("falha_input_nome")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_input",
                }

            # 3) Clica em buscar
            submit_selectors = [
                "input#cirurgiao_submit",
                "button#cirurgiao_submit",
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Buscar')",
            ]
            try:
                submit_btn = _try_select(page, submit_selectors, timeout=10000, steps=steps)
                submit_btn.click(timeout=12000)
                steps.append("clicou:input#cirurgiao_submit")
            except Exception:
                steps.append("falha_submit")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_submit",
                }

            # 4) Espera resultados aparecerem (lista/carta)
            lista_selectors = [
                ".cirurgiao-lista a",
                ".cirurgiao-card a",
                "a:has-text('Ver perfil')",
                ".resultado a",
                ".resultados a",
                ".busca-cirurgiao a",
                "a[href*='cirurgiao']",
            ]
            try:
                # Espera pelo menos um resultado
                primeiro_link = _try_select(page, lista_selectors, timeout=25000, steps=steps)
                steps.append("resultado_apareceu:lista")
            except PWTimeout:
                steps.append("sem_resultados_ou_layout_alterado")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "sem_resultados_ou_layout_alterado",
                }

            # 5) Abre o 1º perfil
            try:
                primeiro_link.click(timeout=12000)
                steps.append("abriu_perfil_primeiro_resultado")
            except Exception as e:
                steps.append(f"erro_click_primeiro_resultado:{e}")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_abrir_perfil",
                }

            # 6) [PATCH 1]: esperar explicitamente o container de perfil
            try:
                page.locator(".cirurgiao-info").first.wait_for(state="visible", timeout=25000)
                steps.append("perfil_container_ok:.cirurgiao-info")
            except Exception as e:
                steps.append(f"perfil_container_timeout:{e}")
                return {
                    "ok": False,
                    "qtd": 0,
                    "resultados": [],
                    "steps": steps,
                    "nome_busca": nome_busca,
                    "timing_ms": int((time.time() - start) * 1000),
                    "reason": "falha_abrir_perfil",
                }

            # 7) [PATCH 2]: extrair via dt/dd e normalizar
            dados = _extract_profile(page, steps)

            # 8) (Opcional) tentar contar resultados a partir da página anterior — aqui fica como 1
            qtd_detectada = 1 if dados else 0

            return {
                "ok": bool(dados),
                "qtd": qtd_detectada,
                "resultados": [],         # se quiser futuramente, pode coletar vários
                "dados": dados,
                "steps": steps,
                "nome_busca": nome_busca,
                "timing_ms": int((time.time() - start) * 1000),
            }

    except PWError as e:
        steps.append(f"playwright_error:{repr(e)}")
        return {
            "ok": False,
            "qtd": 0,
            "resultados": [],
            "steps": steps,
            "nome_busca": nome_busca,
            "timing_ms": int((time.time() - start) * 1000),
            "reason": "playwright_browser_missing",
        }
    except Exception as e:
        steps.append(f"erro_inesperado:{repr(e)}")
        return {
            "ok": False,
            "qtd": 0,
            "resultados": [],
            "steps": steps,
            "nome_busca": nome_busca,
            "timing_ms": int((time.time() - start) * 1000),
            "reason": "erro_inesperado",
        }


# =========================
# Execução direta para testes locais
# =========================
if __name__ == "__main__":
    # Teste rápido: python consulta_medicos.py "NOME DO MEDICO"
    import sys, json
    nome_arg = " ".join(sys.argv[1:]).strip() or "JOAO SILVA"
    out = buscar_sbcp(member_id=None, nome=nome_arg, email=None, steps=[])
    print(json.dumps(out, ensure_ascii=False, indent=2))
