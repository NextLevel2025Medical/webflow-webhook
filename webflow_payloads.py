from flask import Flask, request, jsonify
import json
import os

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
    print("🔔 Webhook recebido:", data)
    save_payload(data)
    return jsonify({"status": "OK"}), 200

@app.route('/', methods=['GET'])
def index():
    return '✅ API Online!', 200

@app.route('/logs', methods=['GET'])
def logs():
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            return f.read(), 200
    return 'Sem logs salvos.', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
