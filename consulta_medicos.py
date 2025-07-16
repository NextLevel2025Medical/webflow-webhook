# === IMPORTS ===
import time
import os
import json
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import subprocess
import sys
from shutil import which

# === LOGGER GLOBAL ===
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("consulta_medicos")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(os.path.join(LOG_DIR, "log_geral.log"), encoding="utf-8")
fh.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
logger.addHandler(fh)

resultado_path = "resultados.json"
recomecar_path = "recomecar_id.txt"
MAX_RESTARTS = 30
restart_count = int(os.environ.get("RESTART_COUNT", 0))
AVG_SECONDS_PER_QUERY = 6

resultado_dicionario = {}

# === HEADLESS SETUP ===
def iniciar_driver_headless():
    from selenium.webdriver.chrome.options import Options

    if not which("google-chrome") and not which("chromium"):
        logger.error("❌ Chrome não está instalado no sistema.")
        sys.exit(1)

    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.binary_location = "/usr/bin/google-chrome"

    try:
        driver = webdriver.Chrome(options=options)
        logger.info("✅ Navegador iniciado com sucesso (headless).")
        return driver
    except Exception as e:
        logger.exception("❌ Erro ao iniciar o ChromeDriver.")
        sys.exit(1)

def salvar_resultados():
    try:
        with open(resultado_path, "w", encoding="utf-8") as res:
            for item in resultado_dicionario.values():
                res.write(json.dumps(item, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.exception(f"Erro ao salvar resultado atualizado: {e}")

def escreve_resultado_json(dado):
    resultado_dicionario[dado["id"]] = dado
    salvar_resultados()

def carregar_resultados_existentes():
    if os.path.exists(resultado_path):
        try:
            with open(resultado_path, "r", encoding="utf-8") as f:
                for linha in f:
                    if linha.strip():
                        dado = json.loads(linha.strip())
                        resultado_dicionario[dado["id"]] = dado
            logger.info(f"Resultados existentes carregados: {len(resultado_dicionario)} registros.")
        except Exception as e:
            logger.exception("Erro ao carregar resultados existentes")

def ids_processados():
    return {
        id_ for id_, dado in resultado_dicionario.items()
        if dado.get("status") == "OK"
    }

def reiniciar_script():
    global restart_count
    restart_count += 1
    if restart_count <= MAX_RESTARTS:
        logger.warning(f"Reiniciando script (tentativa {restart_count})")
        os.environ["RESTART_COUNT"] = str(restart_count)
        subprocess.Popen([sys.executable, __file__], env=os.environ.copy())
    else:
        logger.error("Número máximo de reinícios atingido.")
    sys.exit()

try:
    logger.info("🚨 Subprocesso iniciado — sys.argv: %s", sys.argv)

    carregar_resultados_existentes()

    if len(sys.argv) >= 6:
        id_usuario = int(sys.argv[1])
        nome_busca = sys.argv[2].strip()
        telefone = sys.argv[3].strip()
        email = sys.argv[4].strip()
        data_criacao = sys.argv[5].strip()

        logger.info(f"🚀 Modo individual: {nome_busca} (ID {id_usuario})")

        df = pd.DataFrame([{
            "id": id_usuario,
            "nome": nome_busca,
            "telefone": telefone,
            "email": email,
            "data_criacao": data_criacao
        }])
    else:
        logger.error("⚠️ Argumentos insuficientes. Abortando subprocesso.")
        sys.exit(1)

    driver = iniciar_driver_headless()

    url = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

    for _, row in df.iterrows():
        id_medico = int(row['id'])
        nome_busca = str(row['nome']).strip()

        logger.info(f"🔍 Buscando: {nome_busca} (ID {id_medico})")

        try:
            driver.get(url)
            time.sleep(3)

            campo_nome = driver.find_element(By.ID, "cirurgiao_nome")
            campo_nome.clear()
            campo_nome.send_keys(nome_busca)
            driver.find_element(By.ID, "cirurgiao_submit").click()
            time.sleep(3)

            links_perfil = driver.find_elements(By.CLASS_NAME, "cirurgiao-perfil-link")
            if not links_perfil:
                logger.info("❌ Médico não localizado")
                escreve_resultado_json({
                    "id": id_medico,
                    "nome_busca": nome_busca,
                    "resultados": [],
                    "status": "Não localizado"
                })
                continue

            resultados_extracao = []
            for link in links_perfil:
                try:
                    link.click()
                    time.sleep(2)
                    nome_site = driver.find_element(By.CLASS_NAME, "cirurgiao-nome").text.strip()
                    elementos = driver.find_elements(By.CSS_SELECTOR, ".cirurgiao-info dt, .cirurgiao-info dd")
                    dados_site = {}
                    for i in range(0, len(elementos), 2):
                        chave = elementos[i].text.replace(":", "").strip()
                        valor = elementos[i + 1].text.strip()
                        dados_site[chave] = valor

                    resultado = {
                        "nome_site": nome_site,
                        "email": dados_site.get("Email", ""),
                        "crm": dados_site.get("CRM", ""),
                        "categoria": dados_site.get("Categoria", "")
                    }

                    resultados_extracao.append(resultado)
                    driver.find_element(By.CLASS_NAME, "mfp-close").click()
                    time.sleep(1)

                except Exception as e:
                    logger.warning(f"Erro ao extrair perfil: {e}")
                    continue

            escreve_resultado_json({
                "id": id_medico,
                "nome_busca": nome_busca,
                "resultados": resultados_extracao,
                "status": "OK" if resultados_extracao else "Erro ao extrair perfis"
            })

        except Exception as e:
            logger.exception(f"Erro ao processar '{nome_busca}'")
            escreve_resultado_json({
                "id": id_medico,
                "nome_busca": nome_busca,
                "resultados": [],
                "status": "Erro"
            })

        time.sleep(3)

    logger.info("✅ Consulta concluída.")

except Exception as fatal:
    logger.exception("Erro fatal")

finally:
    try:
        driver.quit()
        logger.info("🧨 Chrome fechado.")
    except:
        pass
