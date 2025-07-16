from flask import Flask, request, jsonify
import json
import os
import subprocess

app = Flask(__name__)
OUTPUT_FILE = 'webflow_payloads.json'

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
    print("🔔 Webhook recebido:", data, flush=True)

    save_payload(data)

    try:
        evento = data.get("event", {})

        id_ = str(evento.get('id', '')).strip()
        nome = str(evento.get('nome', '')).strip()
        telefone = str(evento.get('celular', '')).strip()
        email = str(evento.get('email', '')).strip()
        criado_em = str(data.get('created_at', '')).strip()

        if all([id_, nome, telefone, email, criado_em]):
            print(f"🚀 Chamando subprocesso: {id_}, {nome}, {telefone}, {email}, {criado_em}", flush=True)
            subprocess.Popen([
                'python3', 'consulta_medicos.py',
                id_, nome, telefone, email, criado_em
            ])
        else:
            print("⚠️ Dados incompletos. Subprocesso não iniciado.", flush=True)

    except Exception as e:
        print(f"❌ Erro ao iniciar subprocesso: {e}", flush=True)

    return jsonify({"status": "OK"}), 200

@app.route('/', methods=['GET'])
def index():
    return '✅ API Online!', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
