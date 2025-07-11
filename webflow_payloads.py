from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)

OUTPUT_FILE = 'webflow_payloads.json'

def save_payload(data):
    # Se o arquivo existir, leia o conteúdo atual
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            try:
                all_data = json.load(f)
            except json.JSONDecodeError:
                all_data = []
    else:
        all_data = []

    # Adiciona o novo payload
    all_data.append(data)

    # Salva de volta
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

@app.route('/webflow-webhook', methods=['POST'])
def webflow_webhook():
    data = request.json  # O Webflow envia JSON
    print("🔔 Webhook recebido:", data)

    save_payload(data)

    return jsonify({"status": "OK"}), 200

# ✅ Health check — rota /
@app.route('/', methods=['GET'])
def index():
    return '✅ API Online!', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
