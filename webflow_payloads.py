from flask import Flask, request, jsonify
import json
import os
import subprocess
import uuid
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# === Setup pasta e logger ===
app = Flask(__name__)
OUTPUT_FILE = 'webflow_payloads.json'
TEMP_DIR = 'temp_consultas'
LOG_DIR = 'logs'

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# === Logger configurado ===
LOG_FILE = os.path.join(LOG_DIR, "log_geral.log")
logger = logging.getLogger("webhook_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def save_payload(data):
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            try:
                all_data = json.load(f)
            except json.JSONDecodeError:
                all_data = []
    else:
        all_data = []

    all_data.append(data)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

@app.route('/webflow-webhook', methods=['POST'])
def webflow_webhook():
    data = request.json
    logger.info("🔔 Webhook recebido")

    save_payload(data)

    usuario = data.get("event", {}).get("usuario", {})
    nome = usuario.get("nome")
    id_usuario = usuario.get("id")
    telefone = usuario.get("celular")
    email = usuario.get("email")
    data_criacao = data.get("created_at", datetime.now().isoformat())

    logger.info(f"📥 Dados extraídos: id={id_usuario}, nome={nome}, tel={telefone}, email={email}, criado_em={data_criacao}")

    if nome and id_usuario:
        temp_file = os.path.join(TEMP_DIR, f"{uuid.uuid4().hex}.txt")
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write("id;nome;telefone;email;data_criacao\n")
            f.write(f"{id_usuario};{nome};{telefone};{email};{data_criacao}\n")

        log_individual = os.path.join(LOG_DIR, f"{id_usuario}_consulta.log")
        logger.info(f"📂 Arquivo temporário salvo: {temp_file}")
        logger.info(f"🚀 Iniciando subprocesso para {nome}")

        try:
            subprocess.Popen(
                ["python", "consulta_medicos.py", temp_file],
                stdout=open(log_individual, "w"),
                stderr=subprocess.STDOUT
            )
        except Exception as e:
            logger.error(f"❌ Falha ao iniciar subprocesso: {e}")
    else:
        logger.warning("⚠️ Dados incompletos, não foi possível iniciar validação.")

    return jsonify({"status": "OK"}), 200

@app.route('/', methods=['GET'])
def index():
    return '✅ API Online!', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
