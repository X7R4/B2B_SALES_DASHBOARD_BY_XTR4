import os
import pandas as pd
import streamlit as st
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from fuzzywuzzy import process, fuzz
import unicodedata
import numpy as np
import math
import sys
from datetime import datetime as dt
import time
import json
from workalendar.america import Brazil
import concurrent.futures
import threading
from queue import Queue

# Bibliotecas Google
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ===== INICIALIZAÇÃO DO SESSION STATE =====
# Inicializar session_state ANTES de qualquer outra operação
def init_session_state():
    if "error_messages" not in st.session_state:
        st.session_state.error_messages = []
    if "toast_messages" not in st.session_state:
        st.session_state.toast_messages = []
    if "df_dados" not in st.session_state:
        st.session_state.df_dados = pd.DataFrame()
    if "ultima_atualizacao" not in st.session_state:
        st.session_state.ultima_atualizacao = None
    if "ultima_verificacao" not in st.session_state:
        st.session_state.ultima_verificacao = None
    if "arquivos_info" not in st.session_state:
        st.session_state.arquivos_info = []
    if "primeira_carga" not in st.session_state:
        st.session_state.primeira_carga = True

# Chamar a função de inicialização imediatamente
init_session_state()

# ===== CONFIGURAÇÃO =====
FOLDER_ID = '1FfiukpgvZL92AnRcj1LxE6QW195JLSMY'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# Variável global para controle de sincronização
last_sync_time = None

# Fila para processamento em segundo plano
processing_queue = Queue()
stop_event = threading.Event()

# Função segura para adicionar mensagens de erro
def add_error_message(message):
    try:
        if hasattr(st, 'session_state') and hasattr(st.session_state, 'error_messages'):
            st.session_state.error_messages.append(message)
        else:
            print(f"Erro (session_state não disponível): {message}")
    except Exception as e:
        print(f"Erro ao adicionar mensagem de erro: {e}")

# Função segura para adicionar mensagens de toast
def add_toast_message(message):
    try:
        if hasattr(st, 'session_state') and hasattr(st.session_state, 'toast_messages'):
            st.session_state.toast_messages.append(message)
        else:
            print(f"Toast (session_state não disponível): {message}")
    except Exception as e:
        print(f"Erro ao adicionar mensagem de toast: {e}")

def get_credentials():
    try:
        credentials_info = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
        }
        
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES
        )
        return credentials
    except Exception as e:
        add_error_message(f"Erro ao carregar credenciais: {e}")
        return None

def authenticate_google_drive():
    global last_sync_time
    
    try:
        if last_sync_time and (dt.now() - last_sync_time).total_seconds() < 60:
            return None
            
        credentials_info = {
            "type": st.secrets["gcp_service_account"]["type"],
            "project_id": st.secrets["gcp_service_account"]["project_id"],
            "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
            "private_key": st.secrets["gcp_service_account"]["private_key"],
            "client_email": st.secrets["gcp_service_account"]["client_email"],
            "client_id": st.secrets["gcp_service_account"]["client_id"],
            "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
            "token_uri": st.secrets["gcp_service_account"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
        }
        
        creds = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES
        )
        
        last_sync_time = dt.now()
        return creds
    except Exception as e:
        add_error_message(f"Erro ao carregar credenciais: {e}")
        return None

def list_drive_files(service, folder_id):
    try:
        query = f"parents in '{folder_id}' and mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        page_token = None
        all_files = []
        
        while True:
            results = service.files().list(
                q=query, 
                spaces='drive', 
                fields='nextPageToken, files(id, name, modifiedTime, size)',
                orderBy="modifiedTime desc",
                pageToken=page_token
            ).execute()
            
            files = results.get('files', [])
            all_files.extend(files)
            
            page_token = results.get('nextPageToken', None)
            if not page_token:
                break
                
        return all_files
    except Exception as e:
        add_error_message(f"Erro ao listar arquivos: {e}")
        return []

def download_and_process_file(service, file_info, progress_callback=None):
    try:
        file_id = file_info['id']
        file_name = file_info['name']
        
        if progress_callback:
            progress_callback(f"Baixando {file_name}")
        
        request = service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        if progress_callback:
            progress_callback(f"Processando {file_name}")
        
        try:
            try:
                df = pd.read_excel(file_content, header=None)
            except:
                try:
                    df = pd.read_excel(file_content, header=0)
                except:
                    try:
                        df = pd.read_excel(file_content, header=None, skiprows=5)
                    except Exception as e:
                        add_error_message(f"Não foi possível ler o arquivo {file_name}: {e}")
                        return pd.DataFrame()
            
            result = process_excel_data(df, file_name)
            
            if not result.empty:
                print(f"Arquivo {file_name} processado com sucesso: {len(result)} registros")
            else:
                print(f"Arquivo {file_name} não retornou dados válidos")
            
            return result
        except Exception as e:
            add_error_message(f"Erro ao processar {file_name}: {e}")
            return pd.DataFrame()
    except Exception as e:
        add_error_message(f"Erro ao baixar {file_info['name']}: {e}")
        return pd.DataFrame()

# ===== FUNÇÕES DE PROCESSAMENTO =====

def process_excel_data(df, file_name):
    pedidos = []
    
    try:
        if df.empty or len(df) < 20 or len(df.columns) < 26:
            add_error_message(f"Estrutura inesperada no arquivo {file_name}. Pulando arquivo.")
            return pd.DataFrame()
        
        data_pedido_raw = None
        try:
            data_pedido_raw = df.iloc[1, 15] if len(df) > 1 and len(df.columns) > 15 else None
            if pd.notna(data_pedido_raw):
                data_pedido = pd.to_datetime(data_pedido_raw, errors="coerce", dayfirst=True)
                if pd.isna(data_pedido):
                    data_pedido = None
                else:
                    data_pedido = data_pedido.strftime("%Y-%m-%d")
            else:
                data_pedido = None
        except:
            data_pedido = None
        
        valores_z19_z24 = []
        for i in range(18, 24):
            try:
                if i < len(df) and 25 < len(df.columns):
                    valor = df.iloc[i, 25]
                    if pd.notna(valor):
                        try:
                            valores_z19_z24.append(float(valor))
                        except:
                            pass
            except:
                pass
        
        valor_total_z = sum(valores_z19_z24) if valores_z19_z24 else 0
        
        try:
            numero_pedido = str(df.iloc[1, 8]) if len(df) > 1 and len(df.columns) > 8 else "Desconhecido"
        except:
            numero_pedido = "Desconhecido"
        
        try:
            cliente = str(df.iloc[9, 4]) if len(df) > 9 and len(df.columns) > 4 else "Desconhecido"
        except:
            cliente = "Desconhecido"
        
        try:
            telefone = str(df.iloc[12, 4]) if len(df) > 12 and len(df.columns) > 4 else "Desconhecido"
        except:
            telefone = "Desconhecido"
        
        try:
            cidade = str(df.iloc[11, 4]) if len(df) > 11 and len(df.columns) > 4 else "Desconhecido"
        except:
            cidade = "Desconhecido"
        
        try:
            estado = str(df.iloc[11, 17]) if len(df) > 11 and len(df.columns) > 17 else "Desconhecido"
        except:
            estado = "Desconhecido"
        
        for i in range(18, 24):
            try:
                if i < len(df) and 0 < len(df.columns) and 2 < len(df.columns):
                    quantidade = df.iloc[i, 0]
                    produto = df.iloc[i, 2]
                    
                    if pd.notna(quantidade) and pd.notna(produto):
                        try:
                            qtd = float(quantidade)
                            if qtd > 0:
                                pedidos.append({
                                    "Número do Pedido": numero_pedido,
                                    "Data": data_pedido,
                                    "Cliente": cliente,
                                    "Valor Total Z19-Z24": valor_total_z,
                                    "Produto": str(produto),
                                    "Quantidade": qtd,
                                    "Cidade": cidade,
                                    "Estado": estado,
                                    "Telefone": telefone,
                                    "Arquivo Origem": file_name
                                })
                        except:
                            pass
            except:
                continue
        
    except Exception as e:
        add_error_message(f"Erro ao processar arquivo {file_name}: {e}")
    
    return pd.DataFrame(pedidos)

def verificar_duplicatas(df):
    duplicatas = df[df.duplicated(subset=['Número do Pedido'], keep=False)]
    
    if not duplicatas.empty:
        st.warning(f"Foram encontradas {len(duplicatas)} duplicatas!")
        
        with st.expander("Ver Duplicatas"):
            st.dataframe(duplicatas[["Número do Pedido", "Data", "Cliente", "Valor Total Z19-Z24"]])
        
        st.caption(f"Total de pedidos: {len(df)} | Pedidos únicos: {len(df.drop_duplicates(subset=['Número do Pedido']))} | Duplicatas: {len(duplicatas)}")
        return True
    else:
        st.success("✅ Nenhuma duplicata encontrada!")
        st.caption(f"Total de pedidos: {len(df)} | Todos são únicos")
        return False

def limpar_duplicatas(df):
    df_limpo = df.drop_duplicates(subset=['Número do Pedido', 'Data'])
    
    st.success(f"Removidas {len(df) - len(df_limpo)} duplicatas!")
    st.caption(f"Registros antes: {len(df)} | Registros depois: {len(df_limpo)}")
    
    return df_limpo

def normalize_text(text):
    if pd.isna(text):
        return ""
    text = ''.join(c for c in unicodedata.normalize('NFD', str(text)) if unicodedata.category(c) != 'Mn')
    return text.strip().upper()

def find_closest_city_with_state(city, state, city_list, municipios_df, estados_df, threshold=70):
    if not city or city == "DESCONHECIDO":
        return None, None, None
    
    normalized_city = normalize_text(city)
    normalized_state = normalize_text(state) if state else None
    
    if normalized_state:
        estado_codigo = get_estado_codigo(normalized_state, estados_df)
        if estado_codigo is not None:
            state_cities = municipios_df[municipios_df['codigo_uf'] == estado_codigo]
            state_city_list = state_cities['nome_normalizado'].tolist()
            
            if state_city_list:
                match = process.extractOne(normalized_city, state_city_list, scorer=fuzz.token_sort_ratio)
                if match and match[1] >= threshold:
                    matched_city = match[0]
                    city_info = state_cities[state_cities['nome_normalizado'] == matched_city]
                    if not city_info.empty:
                        return matched_city, city_info.iloc[0]['latitude'], city_info.iloc[0]['longitude']
    
    match = process.extractOne(normalized_city, city_list, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        matched_city = match[0]
        city_info = municipios_df[municipios_df['nome_normalizado'] == matched_city]
        if not city_info.empty:
            if normalized_state:
                estado_codigo = get_estado_codigo(normalized_state, estados_df)
                if estado_codigo is not None and city_info.iloc[0]['codigo_uf'] != estado_codigo:
                    return None, None, None
            return matched_city, city_info.iloc[0]['latitude'], city_info.iloc[0]['longitude']
    
    return None, None, None

def get_estado_codigo(estado_normalizado, estados_df):
    estado_info = estados_df[estados_df['uf_normalizado'] == estado_normalizado]
    if not estado_info.empty:
        return estado_info.iloc[0]['codigo_uf']
    return None

def get_week(data, start_date, end_date):
    total_days = (end_date - start_date).days + 1
    if total_days <= 0 or data < start_date or data > end_date:
        return 0
    days_since_start = (data - start_date).days
    week = ((days_since_start * 4) // total_days) + 1 if days_since_start >= 0 else 0
    return min(max(week, 1), 4)

def classificar_produto(descricao):
    kits_ar = ["KIT 1", "KIT 2", "KIT 3", "KIT 4", "KIT 5", "KIT 6", "KIT 7", 
               "KIT UNIVERSAL", "KIT UPGRADE", "KIT AIR RIDE 4C", "KIT K3", "KIT K4", "KIT K5"]
    descricao_normalizada = str(descricao).strip().upper()
    if any(descricao_normalizada.startswith(kit) for kit in kits_ar):
        return "KITS AR"
    elif "KIT ROSCA" in descricao_normalizada:
        return "KITS ROSCA"
    else:
        return "PEÇAS AVULSAS"

def calcular_comissoes_e_bonus(df, inicio_meta, fim_meta):
    df_periodo = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)].copy()
    df_periodo = df_periodo.drop_duplicates(subset=['Número do Pedido'])
    
    valor_total_vendido = df_periodo["Valor Total Z19-Z24"].sum()
    df_periodo["Categoria"] = df_periodo["Produto"].apply(classificar_produto)
    
    valor_kit_ar = df_periodo[df_periodo["Categoria"] == "KITS AR"]["Valor Total Z19-Z24"].sum()
    valor_pecas_avulsas = df_periodo[df_periodo["Categoria"].isin(["PEÇAS AVULSAS", "KITS ROSCA"])]["Valor Total Z19-Z24"].sum()
    
    percentual_kit_ar = 0.007
    percentual_pecas_avulsas = 0.005
    
    comissao_kit_ar = valor_kit_ar * percentual_kit_ar
    comissao_pecas_avulsas = valor_pecas_avulsas * percentual_pecas_avulsas
    
    bonus = 0
    valor_por_bonus = 200
    quantidade_bonus = int(valor_total_vendido // 50000)
    bonus = quantidade_bonus * valor_por_bonus
    
    meta_atingida = valor_total_vendido >= 200000
    premio_meta = 600 if meta_atingida else 0
    
    ganhos_totais = comissao_kit_ar + comissao_pecas_avulsas + bonus + premio_meta
    
    resultados = pd.DataFrame({
        "Descrição": [
            "Comissão de KIT AR (0.7%)",
            "Comissão de Peças Avulsas e Kit Rosca (0.5%)",
            "Bônus (R$ 200,00 a cada 50 mil vendido)",
            "Prêmio Meta Mensal (se atingida)",
            "Ganhos Estimados"
        ],
        "Valor (R$)": [
            comissao_kit_ar,
            comissao_pecas_avulsas,
            bonus,
            premio_meta,
            ganhos_totais
        ]
    })
    
    return resultados, valor_total_vendido, meta_atingida

def identificar_lojistas_recuperar(df):
    pedidos_por_cliente = df.groupby('Cliente').size().reset_index(name='num_pedidos')
    ultima_compra = df.groupby('Cliente')['Data'].max().reset_index(name='ultima_compra')
    clientes_info = pd.merge(pedidos_por_cliente, ultima_compra, on='Cliente')
    
    hoje = dt.now()
    clientes_info['meses_sem_comprar'] = (hoje - clientes_info['ultima_compra']).dt.days / 30
    
    lojistas_recuperar = clientes_info[
        (clientes_info['num_pedidos'] > 3) & 
        (clientes_info['meses_sem_comprar'] > 3)
    ]
    
    if not lojistas_recuperar.empty:
        lojistas_completos = df.sort_values('Data').drop_duplicates(subset=['Cliente'], keep='last')
        lojistas_recuperar = pd.merge(
            lojistas_recuperar[['Cliente', 'num_pedidos', 'ultima_compra', 'meses_sem_comprar']], 
            lojistas_completos, 
            on='Cliente'
        )
        return lojistas_recuperar
    
    return pd.DataFrame()

def gerar_tabela_pedidos_meta_atual(df, inicio_meta, fim_meta):
    df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)].copy()
    
    if df_meta.empty:
        return pd.DataFrame()
    
    df_meta = df_meta.drop_duplicates(subset=['Número do Pedido'], keep='first')
    tabela = df_meta[["Data", "Número do Pedido", "Cliente", "Valor Total Z19-Z24"]].copy()
    tabela.columns = ["Data do Pedido", "Número do Pedido", "Nome do Cliente", "Valor do Pedido"]
    tabela["Data do Pedido"] = tabela["Data do Pedido"].dt.strftime("%d/%m/%Y")
    tabela = tabela.sort_values("Data do Pedido")
    
    return tabela

# ===== FUNÇÃO DE REFRESH =====

def refresh_drive_data():
    st.cache_data.clear()
    
    global last_sync_time
    last_sync_time = None
    
    if "df_dados" in st.session_state:
        del st.session_state.df_dados
    if "ultima_atualizacao" in st.session_state:
        del st.session_state.ultima_atualizacao
    if "ultima_verificacao" in st.session_state:
        del st.session_state.ultima_verificacao
    if "arquivos_info" in st.session_state:
        del st.session_state.arquivos_info
    
    st.rerun()

def process_files_in_parallel(service, files, max_workers=5, progress_callback=None):
    all_data = []
    total_files = len(files)
    successful_files = 0
    failed_files = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(download_and_process_file, service, file_info, progress_callback): file_info 
            for file_info in files
        }
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                df_result = future.result()
                if not df_result.empty:
                    all_data.append(df_result)
                    successful_files += 1
                else:
                    failed_files += 1
                
                completed += 1
                if progress_callback:
                    progress_callback(f"Concluído {completed}/{total_files}")
            except Exception as e:
                add_error_message(f"Erro ao processar arquivo {file_info['name']}: {e}")
                failed_files += 1
    
    print(f"Resumo do processamento:")
    print(f"- Total de arquivos: {total_files}")
    print(f"- Arquivos processados com sucesso: {successful_files}")
    print(f"- Arquivos sem dados válidos: {failed_files}")
    
    return all_data

def check_new_files():
    creds = authenticate_google_drive()
    if not creds:
        add_error_message("❌ Não foi possível autenticar com o Google Drive")
        return
    
    service = build('drive', 'v3', credentials=creds)
    
    files = list_drive_files(service, FOLDER_ID)
    
    if not files:
        add_error_message("⚠️ Nenhum arquivo encontrado na pasta 'pedidos' do Google Drive")
        return
    
    novos_arquivos = []
    arquivos_atuais = {f['id']: f['modifiedTime'] for f in files}
    
    if "arquivos_info" in st.session_state:
        arquivos_cache = {f['id']: f['modifiedTime'] for f in st.session_state.arquivos_info}
        
        for file_id, modified_time in arquivos_atuais.items():
            if file_id not in arquivos_cache or arquivos_cache[file_id] != modified_time:
                for file_info in files:
                    if file_info['id'] == file_id:
                        novos_arquivos.append(file_info)
                        break
    else:
        novos_arquivos = files
    
    if not novos_arquivos:
        add_toast_message("✅ Nenhum novo arquivo encontrado na pasta 'pedidos'")
        st.session_state.ultima_verificacao = dt.now()
        return
    
    add_toast_message(f"📁 {len(novos_arquivos)} novos arquivos encontrados na pasta 'pedidos'")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    def progress_callback(message):
        status_text.text(message)
        progress_bar.progress(len(novos_arquivos) / len(novos_arquivos))
    
    all_data = process_files_in_parallel(service, novos_arquivos, max_workers=5, progress_callback=progress_callback)
    
    if all_data:
        if "df_dados" in st.session_state:
            df_existente = st.session_state.df_dados
            
            arquivos_novos_nomes = [f['name'] for f in novos_arquivos]
            df_filtrado = df_existente[~df_existente['Arquivo Origem'].isin(arquivos_novos_nomes)]
            
            df_novos = pd.concat(all_data, ignore_index=True)
            final_df = pd.concat([df_filtrado, df_novos], ignore_index=True)
        else:
            final_df = pd.concat(all_data, ignore_index=True)
        
        final_df = final_df.drop_duplicates(subset=['Número do Pedido', 'Data'])
        
        add_toast_message(f"✅ {len(novos_arquivos)} arquivos atualizados! {len(final_df)} pedidos no total")
        
        st.session_state.df_dados = final_df
        st.session_state.arquivos_info = files
        st.session_state.ultima_atualizacao = dt.now()
        st.session_state.ultima_verificacao = dt.now()
        st.session_state.primeira_carga = False
    else:
        add_error_message("⚠️ Nenhum dado válido encontrado nos novos arquivos")

# ===== FUNÇÃO PRINCIPAL DE CARREGAMENTO =====

@st.cache_data(ttl=300)
def carregar_dados_google_drive():
    recarregar = False
    
    if "df_dados" not in st.session_state:
        recarregar = True
        st.session_state.primeira_carga = True
    elif "ultima_verificacao" in st.session_state:
        tempo_desde_ultima_verificacao = (dt.now() - st.session_state.ultima_verificacao).total_seconds()
        if tempo_desde_ultima_verificacao > 300:
            recarregar = True
    else:
        recarregar = True
    
    if recarregar:
        creds = authenticate_google_drive()
        if not creds:
            add_error_message("❌ Não foi possível autenticar com o Google Drive")
            return pd.DataFrame()
        
        service = build('drive', 'v3', credentials=creds)
        
        files = list_drive_files(service, FOLDER_ID)
        
        if not files:
            add_error_message("⚠️ Nenhum arquivo encontrado na pasta 'pedidos' do Google Drive")
            return pd.DataFrame()
        
        novos_arquivos = False
        if "arquivos_info" in st.session_state:
            arquivos_atuais = {f['id']: f['modifiedTime'] for f in files}
            arquivos_cache = {f['id']: f['modifiedTime'] for f in st.session_state.arquivos_info}
            
            if arquivos_atuais != arquivos_cache:
                novos_arquivos = True
        else:
            novos_arquivos = True
        
        if not novos_arquivos and not st.session_state.get('primeira_carga', False):
            st.session_state.ultima_verificacao = dt.now()
            return st.session_state.df_dados
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def progress_callback(message):
            status_text.text(message)
            progress_bar.progress(len(files) / len(files))
        
        all_data = process_files_in_parallel(service, files, max_workers=5, progress_callback=progress_callback)
        
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=['Número do Pedido', 'Data'])
            
            st.session_state.df_dados = final_df
            st.session_state.arquivos_info = files
            st.session_state.ultima_atualizacao = dt.now()
            st.session_state.ultima_verificacao = dt.now()
            st.session_state.primeira_carga = False
            
            return final_df
        else:
            add_error_message("⚠️ Nenhum dado válido encontrado")
            return pd.DataFrame()
    else:
        return st.session_state.df_dados

# ===== FUNÇÃO DE DETECÇÃO AUTOMÁTICA =====

def auto_check_new_files():
    while not stop_event.is_set():
        try:
            if "ultima_verificacao" in st.session_state:
                tempo_desde_ultima_verificacao = (dt.now() - st.session_state.ultima_verificacao).total_seconds()
                if tempo_desde_ultima_verificacao >= 300:
                    processing_queue.put("check_new_files")
            
            time.sleep(60)
        except Exception as e:
            print(f"Erro na verificação automática: {e}")
            time.sleep(60)

def process_queue():
    while not stop_event.is_set():
        try:
            task = processing_queue.get(timeout=1)
            
            if task == "check_new_files":
                creds = authenticate_google_drive()
                if creds:
                    service = build('drive', 'v3', credentials=creds)
                    files = list_drive_files(service, FOLDER_ID)
                    
                    if files:
                        novos_arquivos = []
                        arquivos_atuais = {f['id']: f['modifiedTime'] for f in files}
                        
                        if "arquivos_info" in st.session_state:
                            arquivos_cache = {f['id']: f['modifiedTime'] for f in st.session_state.arquivos_info}
                            
                            for file_id, modified_time in arquivos_atuais.items():
                                if file_id not in arquivos_cache or arquivos_cache[file_id] != modified_time:
                                    for file_info in files:
                                        if file_info['id'] == file_id:
                                            novos_arquivos.append(file_info)
                                            break
                        else:
                            novos_arquivos = files
                        
                        if novos_arquivos:
                            all_data = process_files_in_parallel(service, novos_arquivos, max_workers=5)
                            
                            if all_data:
                                if "df_dados" in st.session_state:
                                    df_existente = st.session_state.df_dados
                                    
                                    arquivos_novos_nomes = [f['name'] for f in novos_arquivos]
                                    df_filtrado = df_existente[~df_existente['Arquivo Origem'].isin(arquivos_novos_nomes)]
                                    
                                    df_novos = pd.concat(all_data, ignore_index=True)
                                    final_df = pd.concat([df_filtrado, df_novos], ignore_index=True)
                                else:
                                    final_df = pd.concat(all_data, ignore_index=True)
                                
                                final_df = final_df.drop_duplicates(subset=['Número do Pedido', 'Data'])
                                
                                st.session_state.df_dados = final_df
                                st.session_state.arquivos_info = files
                                st.session_state.ultima_atualizacao = dt.now()
                                st.session_state.ultima_verificacao = dt.now()
                                st.session_state.primeira_carga = False
                                
                                add_toast_message(f"✅ {len(novos_arquivos)} novos arquivos processados automaticamente!")
            
            processing_queue.task_done()
        except Exception as e:
            print(f"Erro ao processar fila: {e}")

# ===== CONFIGURAÇÃO INICIAL =====

auto_check_thread = threading.Thread(target=auto_check_new_files, daemon=True)
process_queue_thread = threading.Thread(target=process_queue, daemon=True)

auto_check_thread.start()
process_queue_thread.start()

try:
    estados_df = pd.read_csv("estados.csv")
    municipios_df = pd.read_csv("municipios.csv")
    
    municipios_df["nome_normalizado"] = municipios_df["nome"].apply(normalize_text)
    city_list = municipios_df["nome_normalizado"].tolist()
    
    estados_df["uf_normalizado"] = estados_df["uf"].apply(normalize_text)
except Exception as e:
    add_error_message(f"Erro ao carregar arquivos de referência: {e}")
    st.stop()

st.set_page_config(layout="wide", page_title="Dashboard de Vendas")

st.markdown("""
    <style>
        body { 
            background: linear-gradient(135deg, #2C2C2C, #1A1A1A); 
            color: #E0E0E0; 
            font-family: 'Helvetica Neue', Arial, sans-serif; 
            margin: 0; 
            padding: 0; 
            height: 100vh; 
            width: 100vw; 
            overflow-x: hidden; 
        }
        .stProgress > div > div > div > div { 
            background: linear-gradient(90deg, #FF8C00, #FFA500); 
        }
        .stSelectbox, .stMultiselect { 
            background-color: #3A3A3A; 
            border: 1px solid #4A4A4A; 
            border-radius: 8px; 
            color: #E0E0E0; 
            box-shadow: 0 2px 4px rgba(0,0,0,0.2); 
            width: 100%; 
            padding: 8px; 
        }
        .stMetric { 
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A); 
            border: 1px solid #4A4A4A; 
            border-radius: 8px; 
            padding: 15px; 
            color: #E0E0E0; 
            box-shadow: 0 2px 6px rgba(0,0,0,0.3); 
            text-align: center; 
            width: 100%; 
        }
        .section { 
            padding: 25px; 
            margin-bottom: 25px; 
            border-radius: 10px; 
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A); 
            border: 1px solid #4A4A4A; 
            box-shadow: 0 4px 12px rgba(0,0,0,0.3); 
            width: 100%; 
        }
        h1, h2, h3 { 
            color: #FF8C00; 
            font-weight: 500; 
            text-transform: uppercase; 
            letter-spacing: 1px; 
        }
        .stApp { 
            padding: 30px; 
            height: 100%; 
            width: 100%; 
            box-sizing: border-box; 
        }
        .stCaption { 
            color: #B0B0B0; 
            font-size: 0.9em; 
        }
        .css-1aumxhk { 
            width: 100% !important; 
            min-width: 0 !important; 
        }
        .css-1d391kg { 
            width: 100% !important; 
            min-width: 0 !important; 
        }
        .stPlotlyChart { 
            width: 100% !important; 
            height: auto !important; 
        }
        .stSpinner { 
            position: fixed; 
            top: 50%; 
            left: 50%; 
            transform: translate(-50%, -50%); 
            text-align: center; 
            z-index: 1000; 
            width: auto; 
            height: auto; 
        }
        .stTabs [data-baseweb="tab-list"] { 
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A); 
            border-bottom: 1px solid #4A4A4A; 
            padding: 0 10px; 
            display: flex; 
            justify-content: center; 
            margin-bottom: 20px;
        }
        .stTabs [data-baseweb="tab"] { 
            background-color: #3A3A3A; 
            color: #E0E0E0; 
            padding: 10px 20px; 
            margin: 0 5px; 
            border: 1px solid #4A4A4A; 
            border-bottom: none; 
            border-radius: 5px 5px 0 0; 
            cursor: pointer; 
            transition: background-color 0.3s; 
        }
        .stTabs [data-baseweb="tab"]:hover { 
            background-color: #4A4A4A; 
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] { 
            background-color: #2A2A2A; 
            color: #FF8C00; 
            font-weight: bold; 
        }
        .filter-container { 
            display: flex; 
            gap: 15px; 
            align-items: center; 
        }
        .filter-label { 
            font-weight: 500; 
            color: #FF8C00; 
            margin-right: 10px; 
        }
        div[data-testid="stHorizontalBlock"] > div:has(div[data-testid="stPlotlyChart"]) {
            display: flex;
            justify-content: center;
        }
        .filtro-topo {
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A);
            border: 1px solid #4A4A4A;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        .ganhos-destaque {
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A);
            border: 1px solid #4A4A4A;
            border-radius: 8px;
            padding: 15px;
            margin-top: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
            text-align: center;
        }
        .ganhos-valor {
            font-size: 24px;
            font-weight: bold;
            color: #FFA500;
            margin-top: 10px;
        }
        .status-sync {
            background: linear-gradient(135deg, #3A3A3A, #2A2A2A);
            border: 1px solid #4A4A4A;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        .sync-button {
            background: linear-gradient(90deg, #FF8C00, #FFA500);
            color: white;
            border: none;
            border-radius: 8px;
            padding: 12px 24px;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            text-align: center;
            margin: 20px 0;
        }
        .sync-button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.3);
        }
        .creditos {
            text-align: center;
            color: #B0B0B0;
            font-size: 0.9em;
            margin-top: 30px;
            padding: 10px;
        }
        .valor-vermelho {
            color: #FF4444;
            font-weight: bold;
        }
        .valor-azul {
            color: #4A90E2;
            font-weight: bold;
        }
    </style>
""", unsafe_allow_html=True)

# ===== SIDEBAR =====

st.sidebar.title("📊 MENU DE SINCRONIZAÇÃO")

st.sidebar.markdown('<div class="status-sync">', unsafe_allow_html=True)
st.sidebar.markdown("### 🔄 STATUS GOOGLE DRIVE - PASTA 'PEDIDOS'")

st.sidebar.markdown("### ⚙️ CONFIGURAÇÕES DE PROCESSAMENTO")
limitar_arquivos = st.sidebar.checkbox("Limitar número de arquivos", value=False)
max_arquivos = 100

if limitar_arquivos:
    max_arquivos = st.sidebar.number_input(
        "Máximo de arquivos para processar", 
        min_value=10, 
        max_value=5000, 
        value=100,
        step=50
    )

st.sidebar.markdown("### ⚡ OTIMIZAÇÃO")
processamento_paralelo = st.sidebar.checkbox("Processamento paralelo", value=True)
num_workers = st.sidebar.number_input(
    "Número de workers", 
    min_value=1, 
    max_value=10, 
    value=5,
    step=1
)

st.sidebar.markdown("### 🔄 DETECÇÃO AUTOMÁTICA")
auto_detect = st.sidebar.checkbox("Verificar novos arquivos automaticamente", value=True)

df = carregar_dados_google_drive()

if not df.empty:
    st.sidebar.success("✅ Conectado ao Google Drive")
    if "arquivos_info" in st.session_state:
        st.sidebar.caption(f"📁 {len(st.session_state.arquivos_info)} arquivos Excel na pasta 'pedidos'")
        if "ultima_atualizacao" in st.session_state:
            st.sidebar.caption(f"🕒 Última atualização: {st.session_state.ultima_atualizacao.strftime('%d/%m/%Y %H:%M')}")
else:
    st.sidebar.error("❌ Erro na conexão")
    st.sidebar.caption("Verifique a autenticação")

st.sidebar.markdown('</div>', unsafe_allow_html=True)

col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("🔄 Recarregar Dados"):
        refresh_drive_data()
with col2:
    if st.button("🔍 Verificar Novos"):
        check_new_files()

st.sidebar.markdown("---")

# Exibir mensagens de erro
if st.session_state.error_messages:
    for error_msg in st.session_state.error_messages:
        st.error(error_msg)
    st.session_state.error_messages = []

# Exibir mensagens de toast
if st.session_state.toast_messages:
    for toast_msg in st.session_state.toast_messages:
        st.toast(toast_msg)
    st.session_state.toast_messages = []

# ===== CONTEÚDO PRINCIPAL =====

if not df.empty:
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Valor Total Z19-Z24"] = pd.to_numeric(df["Valor Total Z19-Z24"], errors="coerce")
    df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce")
    df["Período_Mês"] = df["Data"].dt.to_period("M")
    df = df.dropna(subset=["Data"])
    
    anos_disponiveis = sorted(df["Data"].dt.year.unique())
    
    hoje = dt.now()
    mes_atual = hoje.month
    ano_atual = hoje.year
    
    tab1, tab2, tab3 = st.tabs(["Desempenho Individual", "Análise de Clientes", "Cálculo de Meta"])
    
    with tab1:
        st.markdown('<div class="filtro-topo">', unsafe_allow_html=True)
        st.markdown("### 📅 FILTRO DOS GRÁFICOS")
        
        col_ano, col_mes = st.columns(2)
        
        with col_ano:
            ano_selecionado = st.selectbox(
                "Ano", 
                anos_disponiveis, 
                index=len(anos_disponiveis)-1,
                key="ano_selecionado"
            )
        
        with col_mes:
            if ano_selecionado:
                meses_disponiveis = sorted(df[df["Data"].dt.year == ano_selecionado]["Data"].dt.month.unique())
            else:
                meses_disponiveis = sorted(df["Data"].dt.month.unique())
            
            nomes_meses = [calendar.month_name[mes] for mes in meses_disponiveis]
            
            if mes_atual in meses_disponiveis and ano_selecionado == ano_atual:
                indice_mes = meses_disponiveis.index(mes_atual)
            else:
                indice_mes = 0
            
            mes_selecionado = st.selectbox(
                "Mês", 
                nomes_meses, 
                index=indice_mes,
                key="mes_selecionado"
            )
            
            mes_selecionado_num = list(calendar.month_name).index(mes_selecionado)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        inicio_periodo_local = dt(ano_selecionado, mes_selecionado_num, 26).replace(hour=0, minute=0, second=0)
        fim_periodo_local = (inicio_periodo_local + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
        df_desempenho_local = df[(df["Data"] >= inicio_periodo_local) & (df["Data"] <= fim_periodo_local)].copy()
        
        col_d1_full, = st.columns([4])
        with col_d1_full:
            vendas_dia = df_desempenho_local.groupby(df_desempenho_local["Data"].dt.date)["Valor Total Z19-Z24"].sum().reset_index()
            fig_dia = px.bar(vendas_dia, x="Data", y="Valor Total Z19-Z24", template="plotly_dark", color_discrete_sequence=["#FF8C00"])
            fig_dia.update_layout(xaxis_title="Data", yaxis_title="Valor Total (R$)", font=dict(size=10), margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_dia, width="stretch")
            
            inicio_atual = dt(ano_selecionado, mes_selecionado_num, 26).replace(hour=0, minute=0, second=0)
            fim_atual = (inicio_atual + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
            inicio_anterior = inicio_atual - relativedelta(years=1)
            fim_anterior = fim_atual - relativedelta(years=1)
            
            df_atual = df[(df["Data"] >= inicio_atual) & (df["Data"] <= fim_atual)].copy()
            df_anterior = df[(df["Data"] >= inicio_anterior) & (df["Data"] <= fim_anterior)].copy()
            
            df_atual["Semana"] = df_atual["Data"].apply(lambda x: get_week(x, start_date=inicio_atual, end_date=fim_atual))
            df_anterior["Semana"] = df_anterior["Data"].apply(lambda x: get_week(x, start_date=inicio_anterior, end_date=fim_anterior))
            
            vendas_atual_week = df_atual.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
            vendas_atual_week["Período"] = vendas_atual_week["Semana"].apply(lambda x: f"Semana {x}")
            vendas_anterior_week = df_anterior.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
            vendas_anterior_week["Período"] = vendas_anterior_week["Semana"].apply(lambda x: f"Semana {x}")
            
            fig_comparacao_ano = go.Figure()
            fig_comparacao_ano.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{ano_selecionado}', line=dict(color='#FF8C00')))
            fig_comparacao_ano.add_trace(go.Scatter(x=vendas_anterior_week["Período"], y=vendas_anterior_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{ano_selecionado-1}', line=dict(color='#FFA500')))
            fig_comparacao_ano.update_layout(
                template="plotly_dark",
                xaxis_title="Semanas",
                yaxis_title="Valor Total (R$)",
                font=dict(size=10),
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_comparacao_ano, width="stretch")
            
            if mes_selecionado_num > 1:
                inicio_mes_anterior = dt(ano_selecionado, mes_selecionado_num - 1, 26).replace(hour=0, minute=0, second=0)
                fim_mes_anterior = (inicio_mes_anterior + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                
                df_mes_anterior = df[(df["Data"] >= inicio_mes_anterior) & (df["Data"] <= fim_mes_anterior)].copy()
                df_mes_anterior["Semana"] = df_mes_anterior["Data"].apply(lambda x: get_week(x, start_date=inicio_mes_anterior, end_date=fim_mes_anterior))
                vendas_mes_anterior_week = df_mes_anterior.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                vendas_mes_anterior_week["Período"] = vendas_mes_anterior_week["Semana"].apply(lambda x: f"Semana {x}")
                
                fig_comparacao_mes = go.Figure()
                fig_comparacao_mes.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[mes_selecionado_num]} {ano_selecionado}', line=dict(color='#FF8C00')))
                fig_comparacao_mes.add_trace(go.Scatter(x=vendas_mes_anterior_week["Período"], y=vendas_mes_anterior_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[mes_selecionado_num-1]} {ano_selecionado}', line=dict(color='#E94F37')))
                fig_comparacao_mes.update_layout(
                    template="plotly_dark",
                    xaxis_title="Semanas",
                    yaxis_title="Valor Total (R$)",
                    font=dict(size=10),
                    margin=dict(l=10, r=10, t=30, b=10),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig_comparacao_mes, width="stretch")
        
        col_d2_full, = st.columns([4])
        with col_d2_full:
            df_periodo = df_desempenho_local.copy()
            df_periodo = df_periodo[df_periodo["Quantidade"] > 0].copy()
            df_periodo["Produto"] = df_periodo["Produto"].str.strip().str.upper()
            top_produtos = df_periodo.groupby("Produto")["Quantidade"].sum().reset_index()
            top_produtos = top_produtos.sort_values(by="Quantidade", ascending=False).head(10)
            
            fig_top_produtos = px.bar(top_produtos, x="Produto", y="Quantidade", 
                                    title=f"Top 10 Produtos Mais Vendidos - {inicio_periodo_local.strftime('%d/%m/%Y')} a {fim_periodo_local.strftime('%d/%m/%Y')}",
                                    template="plotly_dark", color_discrete_sequence=["#FF8C00"])
            fig_top_produtos.update_layout(
                xaxis_title="Produtos",
                yaxis_title="Quantidade Vendida",
                font=dict(size=10),
                margin=dict(l=10, r=10, t=30, b=10),
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig_top_produtos, width="stretch")
            
            df_desempenho_local["Categoria"] = df_desempenho_local["Produto"].apply(classificar_produto)
            vendas_categoria = df_desempenho_local.groupby("Categoria")["Valor Total Z19-Z24"].sum().reset_index()
            categorias_completas = pd.DataFrame({"Categoria": ["KITS AR", "KITS ROSCA", "PEÇAS AVULSAS"]})
            vendas_categoria = pd.merge(categorias_completas, vendas_categoria, on="Categoria", how="left").fillna(0)
            
            fig_categoria = px.pie(vendas_categoria, names="Categoria", values="Valor Total Z19-Z24",
                                 title=f"Vendas por Categoria - {inicio_periodo_local.strftime('%d/%m/%Y')} a {fim_periodo_local.strftime('%d/%m/%Y')}",
                                 template="plotly_dark",
                                 color_discrete_sequence=["#FFA500", "#FF8C00", "#E94F37"])
            fig_categoria.update_traces(textinfo="percent+label", textposition="inside")
            fig_categoria.update_layout(
                font=dict(size=10),
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_categoria, width="stretch")
            
            if st.button("Mostrar Tabela de Pedidos da Meta Atual"):
                if mes_selecionado_num == 1:
                    inicio_meta = dt(ano_selecionado - 1, 12, 26).replace(hour=0, minute=0, second=0)
                    fim_meta = dt(ano_selecionado, 1, 25).replace(hour=23, minute=59, second=59)
                else:
                    inicio_meta = dt(ano_selecionado, mes_selecionado_num - 1, 26).replace(hour=0, minute=0, second=0)
                    fim_meta = dt(ano_selecionado, mes_selecionado_num, 25).replace(hour=23, minute=59, second=59)
                
                tabela_pedidos = gerar_tabela_pedidos_meta_atual(df, inicio_meta, fim_meta)
                if not tabela_pedidos.empty:
                    st.subheader(f"Tabela de Pedidos da Meta Atual ({inicio_meta.strftime('%d/%m/%Y')} a {fim_meta.strftime('%d/%m/%Y')})")
                    
                    verificar_duplicatas(tabela_pedidos)
                    st.dataframe(tabela_pedidos.style.format({'Valor do Pedido': 'R$ {:,.2f}'}), width="stretch")
                    
                    total_unico = tabela_pedidos['Valor do Pedido'].sum()
                    st.caption(f"Valor total de pedidos únicos: R$ {total_unico:,.2f}")
                else:
                    st.warning("Não há pedidos no período da meta atual.")
    
    with tab2:
        df_lojistas_recuperar = identificar_lojistas_recuperar(df)
        
        col_mapa1, col_mapa2 = st.columns([1, 1])
        
        with col_mapa1:
            df_mapa = df.copy()
            df_mapa["Cidade"] = df_mapa["Cidade"].str.strip()
            df_mapa["Estado"] = df_mapa["Estado"].str.strip().str.upper()
            
            df_mapa["Cidade_Corrigida"] = None
            df_mapa["latitude"] = None
            df_mapa["longitude"] = None
            
            for index, row in df_mapa.iterrows():
                cidade = row["Cidade"]
                estado = row["Estado"]
                cidade_corrigida, lat, lon = find_closest_city_with_state(cidade, estado, city_list, municipios_df, estados_df, threshold=70)
                
                if cidade_corrigida and lat and lon:
                    df_mapa.at[index, "Cidade_Corrigida"] = cidade_corrigida
                    df_mapa.at[index, "latitude"] = lat
                    df_mapa.at[index, "longitude"] = lon
                else:
                    estado_normalizado = normalize_text(estado)
                    estado_info = estados_df[estados_df["uf_normalizado"] == estado_normalizado]
                    if not estado_info.empty:
                        df_mapa.at[index, "latitude"] = estado_info.iloc[0]["latitude"]
                        df_mapa.at[index, "longitude"] = estado_info.iloc[0]["longitude"]
                    else:
                        df_mapa.at[index, "latitude"] = -15.7801
                        df_mapa.at[index, "longitude"] = -47.9292
            
            df_mapa["Estado_Corrigido"] = df_mapa["Estado"]
            df_mapa = df_mapa.sort_values('Data').drop_duplicates(subset=['Cliente'], keep='last')
            df_mapa["Coordenadas Atuais"] = df_mapa.apply(lambda row: f"({row['latitude']}, {row['longitude']})", axis=1)
            
            cidades_grupo = df_mapa.groupby("Cidade_Corrigida")
            np.random.seed(42)
            for cidade, grupo in cidades_grupo:
                indices = grupo.index.tolist()
                n_clientes = len(indices)
                
                for i, idx in enumerate(indices):
                    deslocamento_lat = np.random.uniform(-0.002, 0.002)
                    deslocamento_lon = np.random.uniform(-0.002, 0.002)
                    
                    df_mapa.at[idx, "latitude"] += deslocamento_lat
                    df_mapa.at[idx, "longitude"] += deslocamento_lon
            
            df_mapa["Ultima_Compra"] = df_mapa["Data"].dt.strftime("%d/%m/%Y")
            df_mapa = df_mapa.dropna(subset=["latitude", "longitude"])
            
            if not df_mapa.empty:
                with st.spinner("Gerando mapa de localização..."):
                    fig_mapa = go.Figure(go.Scattermap(
                        lat=df_mapa["latitude"],
                        lon=df_mapa["longitude"],
                        mode='markers',
                        hovertemplate=
                        '<b>Cliente</b>: %{customdata[0]}<br>'+
                        '<b>Telefone</b>: %{customdata[1]}<br>'+
                        '<b>Cidade</b>: %{customdata[2]}<br>'+
                        '<b>Estado</b>: %{customdata[3]}<br>'+
                        '<b>Última Compra</b>: %{customdata[4]}<br>'+
                        '<extra></extra>',
                        customdata=df_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra"]],
                        marker=dict(size=7, color="#FF8C00", opacity=0.9,),
                    ))
                    fig_mapa.update_layout(
                        map_style="carto-darkmatter",
                        mapbox_style="dark",
                        mapbox=dict(
                            zoom=3,
                            center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean())
                        ),
                        uirevision="constant",
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        title="Localização dos Clientes",
                        height=600
                    )
                    st.plotly_chart(fig_mapa, width="stretch", config={'scrollZoom': True})
                    
                    df_tabela = df_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Cidade_Corrigida", "Estado_Corrigido", "Coordenadas Atuais"]].copy()
                    st.data_editor(df_tabela, width="stretch")
                    
                    if st.button("Exportar dados dos clientes"):
                        csv = df_tabela.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name='clientes_com_coordenadas.csv',
                            mime='text/csv'
                        )
            else:
                st.warning("Nenhum dado de localização válido após aplicar os filtros.")
        
        with col_mapa2:
            if not df_lojistas_recuperar.empty:
                df_recuperar_mapa = df_lojistas_recuperar.copy()
                df_recuperar_mapa["Cidade"] = df_recuperar_mapa["Cidade"].str.strip()
                df_recuperar_mapa["Estado"] = df_recuperar_mapa["Estado"].str.strip().str.upper()
                
                df_recuperar_mapa["Cidade_Corrigida"] = None
                df_recuperar_mapa["latitude"] = None
                df_recuperar_mapa["longitude"] = None
                
                for index, row in df_recuperar_mapa.iterrows():
                    cidade = row["Cidade"]
                    estado = row["Estado"]
                    cidade_corrigida, lat, lon = find_closest_city_with_state(cidade, estado, city_list, municipios_df, estados_df, threshold=70)
                    
                    if cidade_corrigida and lat and lon:
                        df_recuperar_mapa.at[index, "Cidade_Corrigida"] = cidade_corrigida
                        df_recuperar_mapa.at[index, "latitude"] = lat
                        df_recuperar_mapa.at[index, "longitude"] = lon
                    else:
                        estado_normalizado = normalize_text(estado)
                        estado_info = estados_df[estados_df["uf_normalizado"] == estado_normalizado]
                        if not estado_info.empty:
                            df_recuperar_mapa.at[index, "latitude"] = estado_info.iloc[0]["latitude"]
                            df_recuperar_mapa.at[index, "longitude"] = estado_info.iloc[0]["longitude"]
                        else:
                            df_recuperar_mapa.at[index, "latitude"] = -15.7801
                            df_recuperar_mapa.at[index, "longitude"] = -47.9292
                
                df_recuperar_mapa["Estado_Corrigido"] = df_recuperar_mapa["Estado"]
                df_recuperar_mapa["Ultima_Compra"] = df_recuperar_mapa["Data"].dt.strftime("%d/%m/%Y")
                df_recuperar_mapa = df_recuperar_mapa.dropna(subset=["latitude", "longitude"])
                
                if not df_recuperar_mapa.empty:
                    with st.spinner("Gerando mapa de lojistas a recuperar..."):
                        fig_recuperar = go.Figure(go.Scattermap(
                            lat=df_recuperar_mapa["latitude"],
                            lon=df_recuperar_mapa["longitude"],
                            mode='markers',
                            hovertemplate=
                            '<b>Cliente</b>: %{customdata[0]}<br>'+
                            '<b>Telefone</b>: %{customdata[1]}<br>'+
                            '<b>Cidade</b>: %{customdata[2]}<br>'+
                            '<b>Estado</b>: %{customdata[3]}<br>'+
                            '<b>Última Compra</b>: %{customdata[4]}<br>'+
                            '<b>Meses sem comprar</b>: %{customdata[5]}<br>'+
                            '<extra></extra>',
                            customdata=df_recuperar_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra", "meses_sem_comprar"]],
                            marker=dict(size=9, color="#FFA500", opacity=0.9,),
                        ))
                        fig_recuperar.update_layout(
                            map_style="carto-darkmatter",
                            mapbox_style="dark",
                            mapbox=dict(
                                zoom=3,
                                center=dict(lat=df_recuperar_mapa["latitude"].mean(), lon=df_recuperar_mapa["longitude"].mean())
                            ),
                            uirevision="constant",
                            font=dict(size=10),
                            margin=dict(l=10, r=10, t=30, b=10),
                            title="Lojistas a Recuperar",
                            height=600
                        )
                        st.plotly_chart(fig_recuperar, width="stretch", config={'scrollZoom': True})
                        
                        df_recuperar_tabela = df_recuperar_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra", "meses_sem_comprar"]].copy()
                        df_recuperar_tabela.columns = ["Cliente", "Telefone", "Cidade", "Estado", "Última Compra", "Meses sem Comprar"]
                        st.data_editor(df_recuperar_tabela, width="stretch")
                        
                        if st.button("Exportar dados de lojistas a recuperar"):
                            csv = df_recuperar_tabela.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download CSV",
                                data=csv,
                                file_name='lojistas_a_recuperar.csv',
                                mime='text/csv'
                            )
                else:
                    st.warning("Nenhum dado de localização válido para os lojistas a recuperar.")
            else:
                st.info("Não há lojistas a recuperar no momento. Lojistas a recuperar são aqueles com mais de 3 pedidos e mais de 3 meses sem comprar.")
        
        st.subheader("Análise de Distribuição Geográfica")
        
        regioes_dict = {
            'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
            'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
            'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
            'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul',
            'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste'
        }
        
        df_mapa['Regiao'] = df_mapa['Estado_Corrigido'].map(regioes_dict)
        
        col_pie1, col_pie2 = st.columns([1, 1])
        
        with col_pie1:
            clientes_regiao = df_mapa['Regiao'].value_counts().reset_index()
            clientes_regiao.columns = ['Região', 'Número de Clientes']
            
            fig_regiao = px.pie(clientes_regiao, names='Região', values='Número de Clientes',
                               template='plotly_dark',
                               color_discrete_sequence=['#FF8C00', '#FFA500', '#E94F37', '#F7DC6F', '#BB8FCE'])
            fig_regiao.update_traces(textinfo='percent+label', textposition='inside')
            fig_regiao.update_layout(
                font=dict(size=10),
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=400,
                autosize=True
            )
            st.plotly_chart(fig_regiao, width="stretch")
        
        with col_pie2:
            clientes_estado = df_mapa['Estado_Corrigido'].value_counts().reset_index()
            clientes_estado.columns = ['Estado', 'Número de Clientes']
            top_estados = clientes_estado.head(10)
            
            fig_estado = px.pie(top_estados, names='Estado', values='Número de Clientes',
                               template='plotly_dark',
                               color_discrete_sequence=px.colors.qualitative.Dark24)
            fig_estado.update_traces(textinfo='percent+label', textposition='inside')
            fig_estado.update_layout(
                font=dict(size=10),
                margin=dict(l=10, r=10, t=30, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                height=400,
                autosize=True
            )
            st.plotly_chart(fig_estado, width="stretch")
        
        st.subheader("Análise de Lojistas por Valor Total de Compras")
        
        estados_unicos = sorted(df['Estado'].unique())
        estado_selecionado = st.selectbox("Selecione o estado para análise de lojistas", 
                                         ["Todos"] + estados_unicos,
                                         key="estado_lojistas")
        
        df_lojistas = df.groupby(['Cliente', 'Estado'])['Valor Total Z19-Z24'].sum().reset_index()
        
        if estado_selecionado != "Todos":
            df_lojistas_filtrado = df_lojistas[df_lojistas['Estado'] == estado_selecionado]
            titulo_grafico = f"Top 10 Lojistas - {estado_selecionado}"
        else:
            df_lojistas_filtrado = df_lojistas
            titulo_grafico = "Top 10 Lojistas - Todos os Estados"
        
        top_lojistas = df_lojistas_filtrado.sort_values(by='Valor Total Z19-Z24', ascending=False).head(10)
        
        fig_lojistas = px.bar(top_lojistas, 
                             x='Cliente', 
                             y='Valor Total Z19-Z24',
                             title=titulo_grafico,
                             template='plotly_dark',
                             color_discrete_sequence=['#FF8C00'])
        
        fig_lojistas.update_layout(
            xaxis_title="Lojista",
            yaxis_title="Valor Total de Compras (R$)",
            font=dict(size=10),
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_tickangle=-45
        )
        
        fig_lojistas.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
        
        st.plotly_chart(fig_lojistas, width="stretch")
        
        st.subheader("Dados Detalhados dos Lojistas")
        st.dataframe(top_lojistas.style.format({'Valor Total Z19-Z24': 'R$ {:,.2f}'}), width="stretch")
    
    with tab3:
        st.subheader("CÁLCULO DE META")
        
        st.markdown('<div class="filtro-topo">', unsafe_allow_html=True)
        st.markdown("### 📅 FILTRO DE PERÍODO DA META")
        
        col_ano, col_mes = st.columns(2)
        
        with col_ano:
            ano_meta = st.selectbox(
                "Ano", 
                anos_disponiveis, 
                index=len(anos_disponiveis)-1,
                key="ano_meta"
            )
        
        with col_mes:
            if ano_meta:
                meses_disponiveis = sorted(df[df["Data"].dt.year == ano_meta]["Data"].dt.month.unique())
            else:
                meses_disponiveis = sorted(df["Data"].dt.month.unique())
            
            nomes_meses = [calendar.month_name[mes] for mes in meses_disponiveis]
            
            if mes_atual in meses_disponiveis and ano_meta == ano_atual:
                indice_mes = meses_disponiveis.index(mes_atual)
            else:
                indice_mes = 0
            
            mes_meta = st.selectbox(
                "Mês", 
                nomes_meses, 
                index=indice_mes,
                key="mes_meta"
            )
            
            mes_meta_num = list(calendar.month_name).index(mes_meta)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        if mes_meta_num == 1:
            inicio_meta = dt(ano_meta - 1, 12, 26).replace(hour=0, minute=0, second=0)
            fim_meta = dt(ano_meta, 1, 25).replace(hour=23, minute=59, second=59)
        else:
            inicio_meta = dt(ano_meta, mes_meta_num - 1, 26).replace(hour=0, minute=0, second=0)
            fim_meta = dt(ano_meta, mes_meta_num, 25).replace(hour=23, minute=59, second=59)
        
        df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)]
        
        df_meta_sem_duplicatas = df_meta.drop_duplicates(subset=['Número do Pedido'])
        valor_total_vendido = df_meta_sem_duplicatas["Valor Total Z19-Z24"].sum() if not df_meta_sem_duplicatas.empty else 0
        
        total_pedidos = len(df_meta)
        pedidos_unicos = len(df_meta_sem_duplicatas)
        duplicatas = total_pedidos - pedidos_unicos
        
        meta_total = 200_000
        percentual_meta = min(1.0, valor_total_vendido / meta_total)
        valor_restante = max(0, meta_total - valor_total_vendido)
        
        st.subheader(f"META MENSAL PERÍODO: {inicio_meta.strftime('%d/%m/%Y')} A {fim_meta.strftime('%d/%m/%Y')}")
        st.markdown("<hr style='border: 1px solid #4A4A4A;'>", unsafe_allow_html=True)
        
        st.progress(percentual_meta, text=f"Progresso da Meta: {percentual_meta*100:.1f}%")
        st.caption(f"Número de pedidos processados: {total_pedidos} | Pedidos únicos: {pedidos_unicos} | Duplicatas: {duplicatas}")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Vendido (Z19-Z24)", f"R$ {valor_total_vendido:,.2f}")
        col2.metric("Meta", f"R$ {meta_total:,.2f}")
        col3.metric("Restante", f"R$ {valor_restante:,.2f}")
        
        hoje = dt.now().date()
        
        if hoje < inicio_meta.date():
            dias_uteis_faltantes = 0
            valor_esperado = 0
            valor_diario_necessario = 0
            cor_valor_esperado = "black"
        elif hoje > fim_meta.date():
            dias_uteis_faltantes = 0
            valor_esperado = meta_total
            valor_diario_necessario = 0
            cor_valor_esperado = "black"
        else:
            cal = Brazil()
            dias_uteis_total = cal.get_working_days_delta(inicio_meta.date(), fim_meta.date())
            
            dias_uteis_passados = cal.get_working_days_delta(inicio_meta.date(), hoje)
            
            dias_uteis_faltantes = dias_uteis_total - dias_uteis_passados
            
            if dias_uteis_total > 0:
                valor_esperado = (dias_uteis_passados / dias_uteis_total) * meta_total
            else:
                valor_esperado = meta_total
            
            if dias_uteis_faltantes > 0:
                valor_diario_necessario = (meta_total - valor_total_vendido) / dias_uteis_faltantes
            else:
                valor_diario_necessario = 0
            
            if valor_total_vendido < valor_esperado:
                cor_valor_esperado = "#FF4444"
            else:
                cor_valor_esperado = "#4A90E2"
        
        col4, col5, col6 = st.columns(3)
        
        with col4:
            st.markdown("### Quanto deveria estar")
            if cor_valor_esperado == "#FF4444":
                st.markdown(f'<div class="valor-vermelho">R$ {valor_esperado:,.2f}</div>', unsafe_allow_html=True)
            elif cor_valor_esperado == "#4A90E2":
                st.markdown(f'<div class="valor-azul">R$ {valor_esperado:,.2f}</div>', unsafe_allow_html=True)
            else:
                st.markdown(f"R$ {valor_esperado:,.2f}")
        
        with col5:
            st.markdown("### Dias Úteis Faltantes")
            st.markdown(f"{dias_uteis_faltantes} dias")
        
        with col6:
            st.markdown("### Quanto deve vender por dia")
            st.markdown(f"R$ {valor_diario_necessario:,.2f}")
        
        resultados, valor_total_vendido, meta_atingida = calcular_comissoes_e_bonus(df, inicio_meta, fim_meta)
        
        st.subheader("Detalhamento dos Cálculos")
        st.dataframe(resultados.style.format({'Valor (R$)': 'R$ {:,.2f}'}), width="stretch")
        
        st.markdown('<div class="ganhos-destaque">', unsafe_allow_html=True)
        st.markdown("### Ganhos Estimados")
        ganhos_totais = resultados.iloc[-1, 1]
        st.markdown(f'<div class="ganhos-valor">R$ {ganhos_totais:,.2f}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        with st.expander("Regras de Cálculo"):
            st.markdown("""
            ### Percentuais de Comissão:
            - **KIT AR**: 0.7% (calculado para qualquer valor de vendas)
            - **Peças Avulsas e Kit Rosca**: 0.5% (calculado para qualquer valor de vendas)
            
            ### Bônus:
            - **Bônus por Volume**: R$ 200,00 a cada R$ 50.000,00 vendido
            - **Prêmio Meta Mensal**: R$ 600,00 (se meta de R$ 200.000,00 for atingida)
            
            ### Cálculo dos Novos Campos:
            - **Quanto deveria estar**: Valor esperado com base no progresso dos dias úteis
            - **Dias Úteis Faltantes**: Contagem de dias úteis restantes (excluindo sábados e domingos)
            - **Quanto deve vender por dia**: Valor necessário para atingir a meta nos dias úteis restantes
            """)

else:
    st.warning("⚠️ Nenhum dado disponível. Verifique a configuração do Google Drive.")

st.markdown('<div class="creditos">developed by @joao_vendascastor</div>', unsafe_allow_html=True)

stop_event.set()