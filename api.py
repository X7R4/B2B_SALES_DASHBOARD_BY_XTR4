import streamlit as st
import pandas as pd
import sqlite3
import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest
import threading
import time

# Configurar página para API
st.set_page_config(page_title="Dashboard API", layout="wide")

# Inicializar o banco de dados
def init_db():
    conn = sqlite3.connect('dashboard.db', check_same_thread=False)
    cursor = conn.cursor()
    
    # Criar tabela de pedidos
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        valor_total REAL,
        produto TEXT,
        quantidade REAL,
        cidade TEXT,
        estado TEXT,
        cliente TEXT,
        telefone TEXT,
        arquivo_origem TEXT,
        data_upload TEXT
    )
    ''')
    
    # Criar tabela de log de sincronização
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id TEXT,
        arquivo TEXT,
        data_sync TEXT,
        status TEXT
    )
    ''')
    
    conn.commit()
    return conn

# Inicializar banco de dados
db_conn = init_db()

# Criar aplicação Flask
app = Flask(__name__)

# Função para processar arquivo Excel
def process_excel_file(file_content, filename):
    try:
        # Ler arquivo Excel
        df = pd.read_excel(file_content)
        
        # Processar dados (adaptar conforme sua estrutura)
        dados_processados = []
        
        for _, row in df.iterrows():
            # Adaptar conforme a estrutura real dos seus arquivos
            dados_processados.append({
                "data": row.get("Data", datetime.now()),
                "valor_total": row.get("Valor Total Z19-Z24", 0),
                "produto": row.get("Produto", ""),
                "quantidade": row.get("Quantidade", 0),
                "cidade": row.get("Cidade", ""),
                "estado": row.get("Estado", ""),
                "cliente": row.get("Cliente", ""),
                "telefone": row.get("Telefone", ""),
                "arquivo_origem": filename,
                "data_upload": datetime.now().isoformat()
            })
        
        return dados_processados
    except Exception as e:
        st.error(f"Erro ao processar arquivo {filename}: {str(e)}")
        return []

# Endpoint para upload de arquivos
@app.route('/api/upload', methods=['POST'])
def upload_file():
    try:
        # Verificar autenticação
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({"error": "Token de autenticação inválido"}), 401
        
        token = auth_header.split(' ')[1]
        if token != st.secrets.get("API_TOKEN", "seu_token_secreto"):
            return jsonify({"error": "Token de autenticação inválido"}), 401
        
        # Verificar se tem arquivo
        if 'file' not in request.files:
            return jsonify({"error": "Nenhum arquivo enviado"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "Nenhum arquivo selecionado"}), 400
        
        # Obter ID do cliente
        client_id = request.form.get('client_id', 'unknown')
        
        # Processar arquivo
        dados = process_excel_file(file.read(), file.filename)
        
        # Salvar no banco de dados
        cursor = db_conn.cursor()
        for dado in dados:
            cursor.execute('''
            INSERT INTO pedidos (data, valor_total, produto, quantidade, cidade, estado, cliente, telefone, arquivo_origem, data_upload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                dado["data"],
                dado["valor_total"],
                dado["produto"],
                dado["quantidade"],
                dado["cidade"],
                dado["estado"],
                dado["cliente"],
                dado["telefone"],
                dado["arquivo_origem"],
                dado["data_upload"]
            ))
        
        # Registrar sincronização
        cursor.execute('''
        INSERT INTO sync_log (cliente_id, arquivo, data_sync, status)
        VALUES (?, ?, ?, ?)
        ''', (client_id, file.filename, datetime.now().isoformat(), 'success'))
        
        db_conn.commit()
        
        return jsonify({
            "success": True,
            "message": f"Arquivo {file.filename} processado com sucesso",
            "registros": len(dados)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para verificar status
@app.route('/api/status', methods=['GET'])
def check_status():
    try:
        cursor = db_conn.cursor()
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM pedidos")
        total_registros = cursor.fetchone()[0]
        
        # Obter última sincronização
        cursor.execute("SELECT MAX(data_sync) FROM sync_log")
        ultima_sync = cursor.fetchone()[0]
        
        return jsonify({
            "status": "online",
            "total_registros": total_registros,
            "ultima_sincronizacao": ultima_sync
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint para listar arquivos já sincronizados
@app.route('/api/synced_files', methods=['GET'])
def list_synced_files():
    try:
        cursor = db_conn.cursor()
        
        # Verificar autenticação
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({"error": "Token de autenticação inválido"}), 401
        
        token = auth_header.split(' ')[1]
        if token != st.secrets.get("API_TOKEN", "seu_token_secreto"):
            return jsonify({"error": "Token de autenticação inválido"}), 401
        
        client_id = request.args.get('client_id', 'unknown')
        
        # Obter arquivos já sincronizados
        cursor.execute('''
        SELECT DISTINCT arquivo_origem, MAX(data_sync) as ultima_sync
        FROM sync_log 
        WHERE cliente_id = ? 
        GROUP BY arquivo_origem
        ''', (client_id,))
        
        arquivos = [{"nome": row[0], "ultima_sync": row[1]} for row in cursor.fetchall()]
        
        return jsonify({
            "arquivos": arquivos
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Função para rodar o dashboard
def run_dashboard():
    st.title("Dashboard de Vendas")
    
    # Carregar dados do banco
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM pedidos")
    columns = [description[0] for description in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    
    if not df.empty:
        df['data'] = pd.to_datetime(df['data'])
        
        # Restante do seu código do dashboard aqui
        # ...
        
        # Adicionar aba de administração
        tab1, tab2 = st.tabs(["Dashboard", "Administração"])
        
        with tab1:
            # Seu dashboard normal
            pass
        
        with tab2:
            st.subheader("Logs de Sincronização")
            
            cursor.execute("SELECT * FROM sync_log ORDER BY data_sync DESC LIMIT 100")
            logs = cursor.fetchall()
            
            if logs:
                log_df = pd.DataFrame(logs, columns=['ID', 'Cliente', 'Arquivo', 'Data', 'Status'])
                st.dataframe(log_df)
            else:
                st.info("Nenhum log de sincronização encontrado")
    else:
        st.info("Nenhum dado disponível. Aguardando sincronização de arquivos...")

# Rodar o dashboard em uma thread separada
def run_app():
    app.run(host='0.0.0.0', port=8502, debug=False)

# Iniciar Flask em thread separada
flask_thread = threading.Thread(target=run_app)
flask_thread.daemon = True
flask_thread.start()

# Aguardar o Flask iniciar
time.sleep(2)

# Rodar o dashboard
run_dashboard()