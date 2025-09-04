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
import sys
from datetime import datetime as dt
from workalendar.america import Brazil
import logging
import io
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Configuração de logging detalhada
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dashboard.log')
    ]
)
logger = logging.getLogger(__name__)

# Suprimir avisos específicos do Google API
import warnings
warnings.filterwarnings("ignore", message="file_cache is only supported with oauth2client<4.0.0")

# Bibliotecas Google
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
    logger.info("✅ Bibliotecas Google importadas com sucesso")
except ImportError as e:
    logger.error(f"❌ Erro ao importar bibliotecas Google: {e}")
    st.error("❌ Erro ao importar bibliotecas Google. Verifique a instalação das dependências.")
    st.stop()

# ===== CONFIGURAÇÃO =====
PASTA_ID = "1FfiukpgvZL92AnRcj1LxE6QW195JLSMY"
NOME_PARQUET = "dados_extraidos.parquet"
NOME_CSV = "dados_extraidos.csv"
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

logger.info(f"Configuração inicial - Pasta ID: {PASTA_ID}, Arquivo Parquet: {NOME_PARQUET}, CSV: {NOME_CSV}")


@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
def carregar_dados_google_drive():
    """
    Função final corrigida para carregar dados do Google Drive
    """
    try:
        st.info("🔄 Tentando carregar dados do Google Drive...")
        
        # Método 1: Download direto com URL padrão
        try:
            csv_url = f'https://drive.google.com/uc?export=download&id={PASTA_ID}'
            response = requests.get(csv_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                if not df.empty:
                    st.success("✅ Dados CSV carregados com sucesso!")
                    return processar_dados(df)
        except Exception as e:
            logger.warning(f"Download direto falhou: {e}")
        
        # Método 2: Com service account (se disponível)
        if 'gcp_service_account' in st.secrets:
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
                
                creds = service_account.Credentials.from_service_account_info(
                    credentials_info, scopes=SCOPES
                )
                
                # Baixar arquivo
                service = build('drive', 'v3', credentials=creds)
                request = service.files().get_media(fileId=PASTA_ID)
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                
                while done is False:
                    status, done = downloader.next_chunk()
                
                # Tentar como CSV primeiro
                try:
                    df = pd.read_csv(io.StringIO(file_content.getvalue().decode('utf-8')))
                    st.success("✅ Dados CSV carregados via Service Account!")
                    return processar_dados(df)
                except:
                    # Tentar como Parquet
                    try:
                        df = pd.read_parquet(file_content)
                        st.success("✅ Dados Parquet carregados via Service Account!")
                        return processar_dados(df)
                    except:
                        pass
                        
            except Exception as e:
                logger.warning(f"Service account falhou: {e}")
        
        # Método 3: Download alternativo
        try:
            alt_url = f'https://docs.google.com/uc?export=download&id={PASTA_ID}'
            response = requests.get(alt_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                if not df.empty:
                    st.success("✅ Dados CSV carregados via URL alternativa!")
                    return processar_dados(df)
        except Exception as e:
            logger.warning(f"URL alternativa falhou: {e}")
        
        # Se tudo falhar
        st.error("❌ Nenhuma das tentativas de download funcionou")
        st.info("Soluções:")
        st.markdown("- Verifique se o arquivo está compartilhado com 'Qualquer pessoa com o link'")
        st.markdown("- Confirme o ID do arquivo no Google Drive")
        st.markdown("- Verifique se o arquivo não está corrompido")
        
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Erro crítico no carregamento: {e}")
        st.error(f"Erro crítico: {e}")
        return pd.DataFrame()
def processar_dados(df):
    """
    Processa os dados carregados de forma otimizada
    """
    if df.empty:
        return pd.DataFrame()
    
    # Mapeamento robusto de colunas
    mapeamento_colunas = {
        'data': ['data', 'Data', 'DATA', 'date', 'Date', 'DATE'],
        'valor_total': ['valor_total', 'Valor Total Z19-Z24', 'valor_total', 'Valor Total', 'valor', 'Valor'],
        'quantidade': ['quantidade', 'Quantidade', 'QUANTIDADE', 'qtd', 'QTD'],
        'numero_pedido': ['numero_pedido', 'Número do Pedido', 'pedido', 'Pedido', 'NUMERO_PEDIDO'],
        'cliente': ['cliente', 'Cliente', 'CLIENTE', 'customer', 'Customer'],
        'produto': ['produto', 'Produto', 'PRODUTO', 'item', 'Item'],
        'cidade': ['cidade', 'Cidade', 'CIDADE'],
        'estado': ['estado', 'Estado', 'ESTADO'],
        'telefone': ['telefone', 'Telefone', 'TELEFONE'],
        'valor_unitario': ['valor_unitario', 'Valor Unitário', 'VALOR_UNITARIO', 'unitario', 'Unitário'],
        'valor_produto': ['valor_produto', 'Valor Produto', 'VALOR_PRODUTO', 'produto_value', 'Produto Value']
    }
    
    def encontrar_coluna(df, nomes_esperados):
        for nome in nomes_esperados:
            if nome in df.columns:
                return nome
        return None
    
    # Encontrar colunas correspondentes
    col_data = encontrar_coluna(df, mapeamento_colunas['data'])
    col_valor = encontrar_coluna(df, mapeamento_colunas['valor_total'])
    col_quantidade = encontrar_coluna(df, mapeamento_colunas['quantidade'])
    col_pedido = encontrar_coluna(df, mapeamento_colunas['numero_pedido'])
    col_cliente = encontrar_coluna(df, mapeamento_colunas['cliente'])
    col_produto = encontrar_coluna(df, mapeamento_colunas['produto'])
    col_cidade = encontrar_coluna(df, mapeamento_colunas['cidade'])
    col_estado = encontrar_coluna(df, mapeamento_colunas['estado'])
    col_telefone = encontrar_coluna(df, mapeamento_colunas['telefone'])
    col_unitario = encontrar_coluna(df, mapeamento_colunas['valor_unitario'])
    col_produto_value = encontrar_coluna(df, mapeamento_colunas['valor_produto'])
    
    # Renomear colunas
    df_renomeado = df.copy()
    renomeacoes = {}
    
    if col_data and col_data != 'Data':
        renomeacoes[col_data] = 'Data'
    if col_valor and col_valor != 'Valor Total Z19-Z24':
        renomeacoes[col_valor] = 'Valor Total Z19-Z24'
    if col_quantidade and col_quantidade != 'Quantidade':
        renomeacoes[col_quantidade] = 'Quantidade'
    if col_pedido and col_pedido != 'Número do Pedido':
        renomeacoes[col_pedido] = 'Número do Pedido'
    if col_cliente and col_cliente != 'Cliente':
        renomeacoes[col_cliente] = 'Cliente'
    if col_produto and col_produto != 'Produto':
        renomeacoes[col_produto] = 'Produto'
    if col_cidade and col_cidade != 'Cidade':
        renomeacoes[col_cidade] = 'Cidade'
    if col_estado and col_estado != 'Estado':
        renomeacoes[col_estado] = 'Estado'
    if col_telefone and col_telefone != 'Telefone':
        renomeacoes[col_telefone] = 'Telefone'
    if col_unitario and col_unitario != 'Valor Unitário':
        renomeacoes[col_unitario] = 'Valor Unitário'
    if col_produto_value and col_produto_value != 'Valor Produto':
        renomeacoes[col_produto_value] = 'Valor Produto'
    
    if renomeacoes:
        df_renomeado = df_renomeado.rename(columns=renomeacoes)
        st.info(f"🔄 Colunas renomeadas: {list(renomeacoes.values())}")
    
    df = df_renomeado
    
    # Processar dados
    try:
        logger.info("Processando dados...")
        
        # Converter colunas
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df["Valor Total Z19-Z24"] = pd.to_numeric(df["Valor Total Z19-Z24"], errors="coerce")
        df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce")
        
        # Se coluna Valor Unitário não existir, calcular a partir do Valor Total
        if "Valor Unitário" not in df.columns:
            df["Valor Unitário"] = df.apply(
                lambda row: row["Valor Total Z19-Z24"] / row["Quantidade"] 
                if pd.notna(row["Valor Total Z19-Z24"]) and pd.notna(row["Quantidade"]) and row["Quantidade"] > 0 
                else None,
                axis=1
            )
        
        # Se coluna Valor Produto não existir, calcular
        if "Valor Produto" not in df.columns:
            df["Valor Produto"] = df["Valor Unitário"] * df["Quantidade"]
        
        # Filtrar dados inválidos
        df = df.dropna(subset=["Data", "Valor Produto"])
        df = df[df["Quantidade"] > 0]
        
        # Calcular valor total do pedido por pedido
        df["Valor Total Pedido"] = df.groupby("Número do Pedido")["Valor Produto"].transform("sum")
        
        # Ordenar e remover duplicatas mantendo a última ocorrência
        df = df.sort_values("Data").drop_duplicates(subset=["Número do Pedido", "Produto"], keep="last")
        
        # Adicionar período mensal
        df["Período_Mês"] = df["Data"].dt.to_period("M")
        
        logger.info(f"✅ Dados processados. Shape final: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao processar dados: {e}")
        st.error(f"❌ Erro ao processar dados: {str(e)}")
        return pd.DataFrame()

# ===== FUNÇÕES DE ANÁLISE E VISUALIZAÇÃO =====

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

def verificar_duplicatas(df):
    try:
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
    except Exception as e:
        logger.error(f"Erro ao verificar duplicatas: {e}")
        st.error(f"Erro ao verificar duplicatas: {e}")
        return False

def calcular_comissoes_e_bonus(df, inicio_meta, fim_meta):
    try:
        # Filtrar dados do período
        df_periodo = df[(df['Data'] >= inicio_meta) & (df['Data'] <= fim_meta)].copy()
        
        # Calcular totais
        valor_kit_ar = df_periodo[df_periodo['Produto'].str.contains('KIT', na=False) & 
                                  ~df_periodo['Produto'].str.contains('KIT ROSCA', na=False)]['Valor Produto'].sum()
        valor_pecas_avulsas = df_periodo[df_periodo['Produto'].isin(['PEÇAS AVULSAS', 'KITS ROSCA'])]['Valor Produto'].sum()
        valor_total_vendido = valor_kit_ar + valor_pecas_avulsas
        
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
        
    except Exception as e:
        logger.error(f"Erro ao calcular comissões: {e}")
        st.error(f"Erro ao calcular comissões: {e}")
        return pd.DataFrame(), 0, False

def identificar_lojistas_recuperar(df):
    try:
        # Identificar lojistas com mais de 3 pedidos e mais de 3 meses sem comprar
        hoje = dt.now()
        lojistas_recuperar = df.groupby('Cliente').agg(
            num_pedidos=('Número do Pedido', 'count'),
            ultima_compra=('Data', 'max')
        ).reset_index()
        
        lojistas_recuperar = lojistas_recuperar[
            (lojistas_recuperar['num_pedidos'] > 3) & 
            ((hoje - lojistas_recuperar['ultima_compra']).dt.days > 90)
        ]
        
        # Juntar com dados completos do último pedido
        df_completo = df.sort_values('Data').drop_duplicates(subset=['Cliente'], keep='last')
        lojistas_recuperar = pd.merge(
            lojistas_recuperar[['Cliente', 'num_pedidos', 'ultima_compra']], 
            df_completo, 
            on='Cliente'
        )
        
        return lojistas_recuperar
        
    except Exception as e:
        logger.error(f"Erro ao identificar lojistas: {e}")
        st.error(f"Erro ao identificar lojistas: {e}")
        return pd.DataFrame()

def gerar_tabela_pedidos_meta_atual(df, inicio_meta, fim_meta):
    try:
        # Filtrar pedidos do período
        tabela = df[
            (df['Data'] >= inicio_meta) & (df['Data'] <= fim_meta)
        ][['Data', 'Número do Pedido', 'Cliente', 'Valor Total Pedido']].copy()
        
        tabela = tabela.rename(columns={
            'Data': 'data_pedido',
            'Valor Total Pedido': 'valor_pedido'
        })
        
        if not tabela.empty:
            tabela["data_pedido"] = tabela["data_pedido"].dt.strftime("%d/%m/%Y")
        
        return tabela
        
    except Exception as e:
        logger.error(f"Erro ao gerar tabela de pedidos: {e}")
        st.error(f"Erro ao gerar tabela de pedidos: {e}")
        return pd.DataFrame()

# ===== FUNÇÕES DE PROCESSAMENTO EM LOTES =====

def processar_em_lotes(df, tamanho_lote=1000):
    """
    Processa dados em lotes para melhor performance
    """
    if df.empty:
        return pd.DataFrame()
    
    logger.info(f"Processando dados em lotes de {tamanho_lote} registros...")
    resultados = []
    
    # Dividir DataFrame em lotes
    num_lotes = (len(df) // tamanho_lote) + 1
    for i in range(num_lotes):
        inicio = i * tamanho_lote
        fim = min((i + 1) * tamanho_lote, len(df))
        lote = df.iloc[inicio:fim]
        
        # Processar lote
        lote_processado = processar_lote(lote)
        if not lote_processado.empty:
            resultados.append(lote_processado)
        
        # Mostrar progresso
        progresso = ((i + 1) / num_lotes) * 100
        logger.info(f"Lote {i+1}/{num_lotes} processado ({progresso:.1f}%)")
    
    # Combinar resultados
    if resultados:
        return pd.concat(resultados, ignore_index=True)
    return pd.DataFrame()

def processar_lote(df):
    """
    Processa um lote de dados
    """
    try:
        # Converter colunas
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df["Valor Total Z19-Z24"] = pd.to_numeric(df["Valor Total Z19-Z24"], errors="coerce")
        df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce")
        
        # Calcular valor unitário e valor do produto
        df["Valor Unitário"] = df.apply(
            lambda row: row["Valor Total Z19-Z24"] / row["Quantidade"] 
            if pd.notna(row["Valor Total Z19-Z24"]) and pd.notna(row["Quantidade"]) and row["Quantidade"] > 0 
            else None,
            axis=1
        )
        
        df["Valor Produto"] = df["Valor Unitário"] * df["Quantidade"]
        
        # Filtrar dados inválidos
        df = df.dropna(subset=["Data", "Valor Produto"])
        df = df[df["Quantidade"] > 0]
        
        # Calcular valor total do pedido
        df["Valor Total Pedido"] = df.groupby("Número do Pedido")["Valor Produto"].transform("sum")
        
        return df
        
    except Exception as e:
        logger.error(f"Erro ao processar lote: {e}")
        return pd.DataFrame()

# ===== FUNÇÃO DE CARREGAMENTO PROGRESSIVO =====

def carregar_dados_progressivos():
    """
    Carrega dados de forma progressiva para melhor performance
    """
    try:
        # Carregar dados principais
        df = carregar_dados_google_drive()
        
        if df.empty:
            return pd.DataFrame()
        
        # Processar em lotes para grandes datasets
        if len(df) > 5000:
            st.info(f"Dataset grande ({len(df)} registros). Processando em lotes...")
            df = processar_em_lotes(df, tamanho_lote=2000)
        
        # Consolidar dados
        df_consolidado = consolidar_dados(df)
        
        # Atualizar session state
        st.session_state.df_dados = df_consolidado.copy()
        st.session_state.ultima_atualizacao = dt.now()
        
        return df_consolidado
        
    except Exception as e:
        logger.error(f"Erro no carregamento progressivo: {e}")
        st.error(f"Erro no carregamento progressivo: {e}")
        return pd.DataFrame()

def consolidar_dados(df):
    """
    Consolida dados de forma otimizada
    """
    if df.empty:
        return pd.DataFrame()
    
    try:
        # Agrupar por pedido para calcular métricas
        pedidos = df.groupby('Número do Pedido').agg({
            'Data': 'first',
            'Cliente': 'first',
            'Telefone': 'first',
            'Cidade': 'first',
            'Estado': 'first',
            'Valor Total Pedido': 'sum',
            'Quantidade': 'sum'
        }).reset_index()
        
        # Juntar com detalhes dos produtos
        produtos = df[['Número do Pedido', 'Produto', 'Quantidade', 'Valor Unitário', 'Valor Produto']]
        
        # Remover duplicatas de produtos
        produtos = produtos.drop_duplicates(subset=['Número do Pedido', 'Produto'], keep='last')
        
        return pd.merge(pedidos, produtos, on='Número do Pedido', how='left')
        
    except Exception as e:
        logger.error(f"Erro na consolidação: {e}")
        return df

# ===== CONFIGURAÇÃO INICIAL =====

logger.info("Iniciando configuração inicial do dashboard")

try:
    # Inicializar session_state
    if 'df_dados' not in st.session_state:
        st.session_state.df_dados = pd.DataFrame()
    if 'ultima_atualizacao' not in st.session_state:
        st.session_state.ultima_atualizacao = None
    
    logger.info("Session state inicializado")
    
    # Carregar arquivos de referência
    try:
        logger.info("Carregando arquivos de referência...")
        estados_df = pd.read_csv("estados.csv")
        municipios_df = pd.read_csv("municipios.csv")
        
        municipios_df["nome_normalizado"] = municipios_df["nome"].apply(normalize_text)
        city_list = municipios_df["nome_normalizado"].tolist()
        
        estados_df["uf_normalizado"] = estados_df["uf"].apply(normalize_text)
        logger.info("✅ Arquivos de referência carregados com sucesso")
    except Exception as e:
        logger.error(f"Erro ao carregar arquivos de referência: {e}")
        st.error(f"Erro ao carregar arquivos de referência: {e}")
        st.stop()
    
    # Configurar página
    st.set_page_config(layout="wide", page_title="Dashboard de Vendas com Parquet")
    logger.info("✅ Página configurada")
    
except Exception as e:
    logger.error(f"Erro na configuração inicial: {e}")
    st.error(f"Erro na configuração inicial: {e}")
    st.stop()

# ===== CSS =====
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
    .stPlotlyChart { 
        width: 100% !important; 
        height: auto !important; 
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
    .status-sync {
        background: linear-gradient(135deg, #3A3A3A, #2A2A2A);
        border: 1px solid #4A4A4A;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 20px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
    }
    .creditos {
        text-align: center;
        color: #B0B0B0;
        font-size: 0.9em;
        margin-top: 30px;
        padding: 10px;
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

logger.info("✅ CSS aplicado")

# ===== SIDEBAR =====
st.sidebar.title("📁 MENU DE DADOS - PARQUET/CSV")

st.sidebar.markdown('<div class="status-sync">', unsafe_allow_html=True)
st.sidebar.markdown("### 🔄 STATUS GOOGLE DRIVE")
st.sidebar.markdown(f"### 📄 Arquivo Parquet: {NOME_PARQUET}")
st.sidebar.markdown(f"### 📄 Arquivo CSV: {NOME_CSV}")
st.sidebar.markdown(f"### 📂 Pasta ID: {PASTA_ID}")

if st.sidebar.button("🔄 Recarregar Dados"):
    if 'df_dados' in st.session_state:
        del st.session_state.df_dados
    if 'ultima_atualizacao' in st.session_state:
        del st.session_state.ultima_atualizacao
    st.rerun()

st.sidebar.markdown('</div>', unsafe_allow_html=True)

# ===== CONTEÚDO PRINCIPAL =====

logger.info("Iniciando carregamento de dados principais...")

try:
    # Carregar dados progressivamente
    df = carregar_dados_progressivos()
    logger.info(f"DataFrame carregado. Shape: {df.shape if not df.empty else 'vazio'}")
    
    if df.empty:
        st.error("⚠️ Falha crítica: Nenhum dado foi carregado")
        st.info("Soluções possíveis:")
        st.markdown("- Verifique a conexão com o Google Drive")
        st.markdown("- Confirme se o arquivo existe na pasta")
        st.markdown("- Verifique as permissões de acesso")
        st.stop()
    
    # Seção de status
    st.sidebar.success("✅ Conectado ao Google Drive")
    st.sidebar.caption(f"📁 {len(df)} pedidos carregados")
    
    # Correção robusta para o erro de formatação de data
    ultima_atualizacao_str = "Nenhuma atualização registrada"
    if 'ultima_atualizacao' in st.session_state:
        try:
            if st.session_state.ultima_atualizacao is not None:
                ultima_atualizacao_str = st.session_state.ultima_atualizacao.strftime('%d/%m/%Y %H:%M')
            else:
                ultima_atualizacao_str = "Nenhuma atualização registrada"
        except (AttributeError, TypeError) as e:
            ultima_atualizacao_str = f"Erro de formatação: {str(e)}"
    
    st.sidebar.caption(f"🕒 Última atualização: {ultima_atualizacao_str}")
    
    # ===== DASHBOARD COM ABAS =====
    st.title("📊 Dashboard de Vendas")
    
    if not df.empty:
        anos_disponiveis = sorted(df["Data"].dt.year.unique())
        hoje = dt.now()
        mes_atual = hoje.month
        ano_atual = hoje.year
        
        tab1, tab2, tab3 = st.tabs(["Desempenho Individual", "Análise de Clientes", "Cálculo de Meta"])
        
        # ===== ABA 1: DESEMPENHO INDIVIDUAL =====
        with tab1:
            try:
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
                
                # Gráfico 1: Vendas por dia
                try:
                    col_d1_full, = st.columns([4])
                    with col_d1_full:
                        vendas_dia = df_desempenho_local.groupby(df_desempenho_local["Data"].dt.date)["Valor Total Pedido"].sum().reset_index()
                        fig_dia = px.bar(vendas_dia, x="Data", y="Valor Total Pedido", template="plotly_dark", color_discrete_sequence=["#FF8C00"])
                        fig_dia.update_layout(xaxis_title="Data", yaxis_title="Valor Total (R$)", font=dict(size=10), margin=dict(l=10, r=10, t=30, b=10))
                        st.plotly_chart(fig_dia, width="stretch")
                        logger.info("✅ Gráfico de vendas por dia criado")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de vendas por dia: {e}")
                    st.error(f"Erro ao criar gráfico de vendas por dia: {e}")
                
                # Gráfico 2: Comparação anual
                try:
                    inicio_atual = dt(ano_selecionado, mes_selecionado_num, 26).replace(hour=0, minute=0, second=0)
                    fim_atual = (inicio_atual + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    inicio_anterior = inicio_atual - relativedelta(years=1)
                    fim_anterior = fim_atual - relativedelta(years=1)
                    
                    df_atual = df[(df["Data"] >= inicio_atual) & (df["Data"] <= fim_atual)].copy()
                    df_anterior = df[(df["Data"] >= inicio_anterior) & (df["Data"] <= fim_anterior)].copy()
                    
                    df_atual["Semana"] = df_atual["Data"].apply(lambda x: get_week(x, start_date=inicio_atual, end_date=fim_atual))
                    df_anterior["Semana"] = df_anterior["Data"].apply(lambda x: get_week(x, start_date=inicio_anterior, end_date=fim_anterior))
                    
                    vendas_atual_week = df_atual.groupby("Semana")["Valor Total Pedido"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                    vendas_atual_week["Período"] = vendas_atual_week["Semana"].apply(lambda x: f"Semana {x}")
                    vendas_anterior_week = df_anterior.groupby("Semana")["Valor Total Pedido"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                    vendas_anterior_week["Período"] = vendas_anterior_week["Semana"].apply(lambda x: f"Semana {x}")
                    
                    fig_comparacao_ano = go.Figure()
                    fig_comparacao_ano.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Pedido"], mode='lines+markers', name=f'{ano_selecionado}', line=dict(color='#FF8C00')))
                    fig_comparacao_ano.add_trace(go.Scatter(x=vendas_anterior_week["Período"], y=vendas_anterior_week["Valor Total Pedido"], mode='lines+markers', name=f'{ano_selecionado-1}', line=dict(color='#FFA500')))
                    fig_comparacao_ano.update_layout(
                        template="plotly_dark",
                        xaxis_title="Semanas",
                        yaxis_title="Valor Total (R$)",
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_comparacao_ano, width="stretch")
                    logger.info("✅ Gráfico de comparação anual criado")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de comparação anual: {e}")
                    st.error(f"Erro ao criar gráfico de comparação anual: {e}")
                
                # Gráfico 3: Top produtos
                try:
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
                        logger.info("✅ Gráfico de top produtos criado")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de top produtos: {e}")
                    st.error(f"Erro ao criar gráfico de top produtos: {e}")
                
                # Gráfico 4: Vendas por categoria
                try:
                    df_desempenho_local["Categoria"] = df_desempenho_local["Produto"].apply(classificar_produto)
                    vendas_categoria = df_desempenho_local.groupby("Categoria")["Valor Total Pedido"].sum().reset_index()
                    categorias_completas = pd.DataFrame({"Categoria": ["KITS AR", "KITS ROSCA", "PEÇAS AVULSAS"]})
                    vendas_categoria = pd.merge(categorias_completas, vendas_categoria, on="Categoria", how="left").fillna(0)
                    
                    fig_categoria = px.pie(vendas_categoria, names="Categoria", values="Valor Total Pedido",
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
                    logger.info("✅ Gráfico de vendas por categoria criado")
                except Exception as e:
                    logger.error(f"Erro ao criar gráfico de vendas por categoria: {e}")
                    st.error(f"Erro ao criar gráfico de vendas por categoria: {e}")
                
            except Exception as e:
                logger.error(f"Erro na aba Desempenho Individual: {e}")
                st.error(f"Erro na aba Desempenho Individual: {e}")
        
        # ===== ABA 2: ANÁLISE DE CLIENTES =====
        with tab2:
            try:
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
                        logger.info("✅ Mapa de clientes criado")
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
                            logger.info("✅ Mapa de lojistas a recuperar criado")
                        else:
                            st.warning("Nenhum dado de localização válido para os lojistas a recuperar.")
                    else:
                        st.info("Não há lojistas a recuperar no momento. Lojistas a recuperar são aqueles com mais de 3 pedidos e mais de 3 meses sem comprar.")
                
                # Gráficos de distribuição geográfica
                try:
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
                        logger.info("✅ Gráfico de distribuição por região criado")
                    
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
                        logger.info("✅ Gráfico de distribuição por estado criado")
                    
                    # Análise de lojistas por valor
                    st.subheader("Análise de Lojistas por Valor Total de Compras")
                    
                    estados_unicos = sorted(df['Estado'].unique())
                    estado_selecionado = st.selectbox("Selecione o estado para análise de lojistas", 
                                                     ["Todos"] + estados_unicos,
                                                     key="estado_lojistas")
                    
                    df_lojistas = df.groupby(['Cliente', 'Estado'])['Valor Total Pedido'].sum().reset_index()
                    
                    if estado_selecionado != "Todos":
                        df_lojistas_filtrado = df_lojistas[df_lojistas['Estado'] == estado_selecionado]
                        titulo_grafico = f"Top 10 Lojistas - {estado_selecionado}"
                    else:
                        df_lojistas_filtrado = df_lojistas
                        titulo_grafico = "Top 10 Lojistas - Todos os Estados"
                    
                    top_lojistas = df_lojistas_filtrado.sort_values(by='Valor Total Pedido', ascending=False).head(10)
                    
                    fig_lojistas = px.bar(top_lojistas, 
                                         x='Cliente', 
                                         y='Valor Total Pedido',
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
                    st.dataframe(top_lojistas.style.format({'Valor Total Pedido': 'R$ {:,.2f}'}), width="stretch")
                    logger.info("✅ Análise de lojistas criada")
                    
                except Exception as e:
                    logger.error(f"Erro na análise de distribuição geográfica: {e}")
                    st.error(f"Erro na análise de distribuição geográfica: {e}")
                
            except Exception as e:
                logger.error(f"Erro na aba Análise de Clientes: {e}")
                st.error(f"Erro na aba Análise de Clientes: {e}")
        
        # ===== ABA 3: CÁLCULO DE META =====
        with tab3:
            try:
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
                
                # Calcular dados da meta
                df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)].copy()
                total_pedidos = len(df_meta)
                pedidos_unicos = df_meta['Número do Pedido'].nunique()
                duplicatas = total_pedidos - pedidos_unicos
                valor_total_vendido = df_meta['Valor Total Pedido'].sum()
                
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
                
                # Cálculo de comissões
                try:
                    resultados, valor_total_vendido, meta_atingida = calcular_comissoes_e_bonus(df, inicio_meta, fim_meta)
                    
                    st.subheader("Detalhamento dos Cálculos")
                    st.dataframe(resultados.style.format({'Valor (R$)': 'R$ {:,.2f}'}), width="stretch")
                    
                    st.markdown('<div class="ganhos-destaque">', unsafe_allow_html=True)
                    st.markdown("### Ganhos Estimados")
                    ganhos_totais = resultados.iloc[-1, 1]
                    st.markdown(f'<div class="ganhos-valor">R$ {ganhos_totais:,.2f}</div>', unsafe_allow_html=True)
                    st.markdown('</div>', unsafe_allow_html=True)
                    logger.info("✅ Cálculo de meta e comissões criado")
                    
                except Exception as e:
                    logger.error(f"Erro no cálculo de comissões: {e}")
                    st.error(f"Erro no cálculo de comissões: {e}")
                
                # Tabela de pedidos
                try:
                    if st.button("Mostrar Tabela de Pedidos da Meta Atual"):
                        if mes_selecionado_num == 1:
                            inicio_meta_tabela = dt(ano_meta - 1, 12, 26).replace(hour=0, minute=0, second=0)
                            fim_meta_tabela = dt(ano_meta, 1, 25).replace(hour=23, minute=59, second=59)
                        else:
                            inicio_meta_tabela = dt(ano_meta, mes_meta_num - 1, 26).replace(hour=0, minute=0, second=0)
                            fim_meta_tabela = dt(ano_meta, mes_meta_num, 25).replace(hour=23, minute=59, second=59)
                        
                        tabela_pedidos = gerar_tabela_pedidos_meta_atual(df, inicio_meta_tabela, fim_meta_tabela)
                        if not tabela_pedidos.empty:
                            st.subheader(f"Tabela de Pedidos da Meta Atual ({inicio_meta_tabela.strftime('%d/%m/%Y')} a {fim_meta_tabela.strftime('%d/%m/%Y')})")
                            
                            verificar_duplicatas(tabela_pedidos)
                            st.dataframe(tabela_pedidos.style.format({'Valor do Pedido': 'R$ {:,.2f}'}), width="stretch")
                            
                            total_unico = tabela_pedidos['Valor do Pedido'].sum()
                            st.caption(f"Valor total de pedidos únicos: R$ {total_unico:,.2f}")
                        else:
                            st.warning("Não há pedidos no período da meta atual.")
                        logger.info("✅ Tabela de pedidos criada")
                    
                except Exception as e:
                    logger.error(f"Erro ao criar tabela de pedidos: {e}")
                    st.error(f"Erro ao criar tabela de pedidos: {e}")
                
            except Exception as e:
                logger.error(f"Erro na aba Cálculo de Meta: {e}")
                st.error(f"Erro na aba Cálculo de Meta: {e}")
    
    else:
        st.warning("⚠️ Nenhum dado disponível. Verifique a configuração do Google Drive.")
    
    # Rodapé
    st.markdown('<div class="creditos">developed by @joao_vendascastor</div>', unsafe_allow_html=True)
    logger.info("Dashboard finalizado")
    
except Exception as e:
    logger.error(f"Erro crítico no dashboard: {e}", exc_info=True)
    st.error(f"❌ Erro crítico: {str(e)}")
    st.write("Detalhes do erro:")
    st.write(f"Tipo: {type(e).__name__}")
    st.write(f"Mensagem: {str(e)}")