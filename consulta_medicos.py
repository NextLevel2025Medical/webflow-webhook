import time
import os
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import subprocess
import sys

# === CONFIGURAÇÕES ===
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_path = os.path.join(LOG_DIR, "log_busca_medicos.txt")
resultado_path = "resultados.json"
recomecar_path = "recomecar_id.txt"

MAX_RESTARTS = 30
restart_count = int(os.environ.get("RESTART_COUNT", 0))
AVG_SECONDS_PER_QUERY = 6

resultado_dicionario = {}

# === LOG PADRÃO ===
def escreve_log(texto):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    linha = f"{timestamp} {texto}"
    print(linha)
    with open(log_path, "a", encoding="utf-8") as log:
        log.write(linha + "\n")

# === FUNÇÕES DE ARQUIVO ===
def salvar_resultados():
    try:
        with open(resultado_path, "w", encoding="utf-8") as res:
            for item in resultado_dicionario.values():
                res.write(json.dumps(item, ensure_ascii=False) + "\n")
    except Exception as e:
        escreve_log(f"❌ Erro ao salvar resultado atualizado: {e}")

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
            escreve_log(f"🔁 Resultados existentes carregados: {len(resultado_dicionario)} registros.")
        except Exception as e:
            escreve_log(f"❌ Erro ao carregar resultados existentes: {e}")

def ids_processados():
    return {
        id_ for id_, dado in resultado_dicionario.items()
        if dado.get("status") == "OK"
    }

def reiniciar_script():
    global restart_count
    restart_count += 1
    if restart_count <= MAX_RESTARTS:
        escreve_log(f"🔄 Reiniciando o script automaticamente... (Tentativa {restart_count})")
        os.environ["RESTART_COUNT"] = str(restart_count)
        subprocess.Popen([sys.executable, __file__], env=os.environ.copy())
    else:
        escreve_log("🛑 Número máximo de reinícios atingido. Abortando.")
    sys.exit()

# === INÍCIO DO SCRIPT ===
try:
    with open(log_path, "a", encoding="utf-8") as log:
        log.write("=== NOVA EXECUÇÃO ===\n")

    escreve_log("Iniciando script...")

    carregar_resultados_existentes()

    # Verifica se foi chamado com argumentos
    if len(sys.argv) >= 6:
        modo_individual = True
        id_usuario = int(sys.argv[1])
        nome_busca = sys.argv[2]
        telefone = sys.argv[3]
        email = sys.argv[4]
        data_criacao = sys.argv[5]

        escreve_log(f"🚀 Modo individual: {nome_busca} (ID {id_usuario})")

        df = pd.DataFrame([{
            "id": id_usuario,
            "nome": nome_busca,
            "telefone": telefone,
            "email": email,
            "data_criacao": data_criacao
        }])
    else:
        escreve_log("📄 Modo em lote ativado (arquivo medicos.txt)")
        df = pd.read_csv("medicos.txt", sep=";", encoding="utf-8")
        df["id"] = df["id"].astype(int)

        recomecar_id = None
        if os.path.exists(recomecar_path):
            try:
                with open(recomecar_path, "r", encoding="utf-8") as f:
                    recomecar_id = int(f.read().strip())
                escreve_log(f"🔁 Reinício forçado a partir do ID {recomecar_id}")
            except Exception as e:
                escreve_log(f"⚠️ Falha ao ler recomecar_id.txt: {e}")

        if recomecar_id:
            idx_inicio = df[df["id"] == recomecar_id].index
            if not idx_inicio.empty:
                df = df.loc[idx_inicio[0]:].copy()
            else:
                escreve_log("⚠️ ID de reinício não encontrado. Executando normalmente.")
            os.remove(recomecar_path)
        else:
            processados = ids_processados()
            df = df[~df["id"].isin(processados)]

        total_restante = len(df)
        estimativa_seg = total_restante * AVG_SECONDS_PER_QUERY
        hora_prevista = datetime.now() + timedelta(seconds=estimativa_seg)
        escreve_log(f"📊 Total a processar: {total_restante}")
        escreve_log(f"⏳ Estimativa: ~{estimativa_seg // 60:.1f} min. Término ≈ {hora_prevista.strftime('%H:%M')}")

    # === SELENIUM ===
    driver = webdriver.Chrome()
    driver.maximize_window()
    escreve_log("🧭 Navegador iniciado")

    url = "https://www.cirurgiaplastica.org.br/encontre-um-cirurgiao/#busca-cirurgiao"

    for pos, (_, row) in enumerate(df.iterrows(), start=1):
        id_medico = int(row['id'])
        nome_busca = str(row['nome']).strip()

        escreve_log(f"\n🔍 ({pos}) Buscando: {nome_busca} (ID {id_medico})")

        try:
            driver.get(url)
            time.sleep(3)

            campo_nome = driver.find_element(By.ID, "cirurgiao_nome")
            campo_nome.clear()
            campo_nome.send_keys(nome_busca)

            try:
                driver.find_element(By.ID, "cirurgiao_submit").click()
            except Exception as click_error:
                if "element click intercepted" in str(click_error):
                    escreve_log("⚠️ Clique interceptado. Salvando ID e reiniciando...")

                    escreve_resultado_json({
                        "id": id_medico,
                        "nome_busca": nome_busca,
                        "resultados": [],
                        "status": "Erro clique interceptado"
                    })

                    with open(recomecar_path, "w", encoding="utf-8") as f:
                        f.write(str(id_medico))

                    driver.quit()
                    reiniciar_script()
                else:
                    raise click_error

            time.sleep(3)
            links_perfil = driver.find_elements(By.CLASS_NAME, "cirurgiao-perfil-link")

            if not links_perfil:
                escreve_log("❌ Médico não localizado")
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
                    time.sleep(3)

                    try:
                        nome_site = driver.find_element(By.CLASS_NAME, "cirurgiao-nome").text.strip()
                    except:
                        nome_site = ""

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
                    time.sleep(2)

                except Exception as e:
                    escreve_log(f"❌ Erro ao extrair perfil: {e}")
                    continue

            status_final = "OK" if resultados_extracao else "Erro ao extrair perfis"
            escreve_resultado_json({
                "id": id_medico,
                "nome_busca": nome_busca,
                "resultados": resultados_extracao,
                "status": status_final
            })

            time.sleep(3)

        except Exception as e:
            escreve_log(f"❌ Erro ao processar '{nome_busca}': {e}")
            escreve_resultado_json({
                "id": id_medico,
                "nome_busca": nome_busca,
                "resultados": [],
                "status": "Erro"
            })

    escreve_log("✅ Todas as buscas concluídas.")

except Exception as fatal:
    escreve_log(f"🛑 Erro fatal: {fatal}")

finally:
    try:
        driver.quit()
        escreve_log("🧨 Navegador fechado.")
    except:
        pass
    if not len(sys.argv) >= 6:
        input("\nPressione ENTER para sair...")
