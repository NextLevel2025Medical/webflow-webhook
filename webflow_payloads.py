# --- topo do arquivo ---
from flask import Flask, request, jsonify
import json, os, subprocess

# NEW
from sqlalchemy import create_engine, text

app = Flask(__name__)
OUTPUT_FILE = 'webflow_payloads.json'

# NEW: conecta no Neon via env var
DB_URL = os.getenv("DATABASE_URL")
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None

def save_payload(data):
    # (seu código atual) - mantém debug local
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

# NEW: persiste no Postgres -> tabela membersnextlevel
def persist_db(data: dict):
    if not engine:
        return
    evento = (data or {}).get("event", {}) or {}
    row = {
        "event_id":   str(evento.get("id") or ""),
        "nome":       str(evento.get("nome") or ""),
        "email":      str(evento.get("email") or ""),
        "celular":    str(evento.get("celular") or ""),
        "created_at": str((data or {}).get("created_at") or ""),
        "raw":        json.dumps(data, ensure_ascii=False),
    }
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO membersnextlevel
            (event_id, nome, email, celular, created_at, raw)
            VALUES (:event_id, :nome, :email, :celular, :created_at, :raw)
        """), row)

@app.route('/webflow-webhook', methods=['POST'])
def webflow_webhook():
    data = request.json
    print("🔔 Webhook recebido:", data, flush=True)

    save_payload(data)                               # mantém arquivo local (debug)  
    try:
        persist_db(data)                             # grava no Neon
    except Exception as e:
        print(f"⚠️ Falha ao gravar no DB: {e}", flush=True)

    try:
        evento = data.get("event", {})               # seu fluxo atual…               
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
