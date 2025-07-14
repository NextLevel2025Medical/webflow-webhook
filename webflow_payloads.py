from flask import Flask, request, jsonify
import json
import os
import subprocess
import uuid
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# === CONFIGURAÇÃO DE PASTAS ===
app = Flask(__name__)
OUTPUT_FILE = 'webflow_payloads.json'
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

# === CONFIGURAÇÃO DO LOGGER CENTRAL ===
LOG_FILE = os.path.join(LOG_DIR, "log_geral.log")
logger = logging.getLogger("webhook_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# === SALVA PAYLOAD RECEBIDO ===
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

# === ROTA PRINCIPAL DO WEBHOOK ===
@app.route('/webflow-webhook', methods=['POST'])
def webflow_webhook():
    data = request.json
    logger.info("🔔 Webhook recebido.")

    save_payload(data)

    usuario = data.get("event", {}).get("usuario", {})
    nome = usuario.get("nome", "").strip()
    id_usuario = usuario.get("id")
    telefone = usuario.get("celular", "")
    email = usuario.get("email", "")
    data_criacao = data.get("created_at", datetime.now().isoformat())

    logger.info(f"📥 Extraído: id={id_usuario}, nome='{nome}', tel='{telefone}', email='{email}', criado_em='{data_criacao}'")

    if nome and id_usuario:
        log_individual = os.path.join(LOG_DIR, f"{id_usuario}_consulta.log")
        logger.info(f"🚀 Chamando subprocesso para '{nome}' (ID {id_usuario})")

        try:
            subprocess.Popen(
                ["python", "consulta_medicos.py", str(id_usuario), nome, telefone or "", email or "", data_criacao],
                stdout=open(log_individual, "w"),
                stderr=subprocess.STDOUT
            )
        except Exception as e:
            logger.error(f"❌ Erro ao iniciar subprocesso: {e}")
    else:
        logger.warning("⚠️ Dados insuficientes para iniciar subprocesso.")

    return jsonify({"status": "OK"}), 200

# === HEALTH CHECK ===
@app.route('/', methods=['GET'])
def index():
    return '✅ API Online!', 200

# === INICIAR SERVIDOR ===
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
