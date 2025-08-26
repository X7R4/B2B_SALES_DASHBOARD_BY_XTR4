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
import requests
import json
from datetime import datetime as dt
import time
 
# Função para obter caminho de recursos (essencial para PyInstaller)
def resource_path(relative_path):
    """ Obter caminho absoluto para recursos, funciona para dev e PyInstaller """
    try:
        # PyInstaller cria uma pasta temporária e armazena o caminho em _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)
 
# Modificar os caminhos dos arquivos CSV
estados_csv_path = resource_path("estados.csv")
municipios_csv_path = resource_path("municipios.csv")
 
# Função para obter a URL da API a partir de um arquivo ou configuração
def obter_url_api():
    """Obtém a URL da API a partir de um arquivo ou configuração"""
    # Tentar obter a URL do arquivo ngrok_url.txt
    if os.path.exists("ngrok_url.txt"):
        with open("ngrok_url.txt", "r") as f:
            return f.read().strip()
    
    # Se não existir, usar localhost
    return "http://localhost:8000"
 
# Configuração da API local - agora dinâmica
API_URL = obter_url_api()
 
# Funções de integração com API
def verificar_api_local():
    """Verifica se a API local está acessível"""
    try:
        response = requests.get(f"{API_URL}/api/saude", timeout=5)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, {"erro": f"Status code: {response.status_code}"}
    except Exception as e:
        return False, {"erro": str(e)}
 
def obter_arquivos_api():
    """Obtém a lista de arquivos da API local"""
    try:
        response = requests.get(f"{API_URL}/api/arquivos", timeout=10)
        if response.status_code == 200:
            return response.json().get("arquivos", [])
        else:
            return []
    except Exception as e:
        return []
 
def obter_dados_arquivo_api(nome_arquivo):
    """Obtém os dados de um arquivo específico da API local"""
    try:
        response = requests.get(f"{API_URL}/api/arquivo/{nome_arquivo}", timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return None
 
def obter_todos_os_pedidos_api():
    """Obtém todos os pedidos de todos os arquivos da API local"""
    try:
        response = requests.get(f"{API_URL}/api/todos-pedidos", timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            return {"pedidos": [], "total": 0}
    except Exception as e:
        return {"pedidos": [], "total": 0}
 
def converter_dados_api_para_dataframe(dados_api):
    """Converte os dados da API para o formato DataFrame esperado pelo dashboard"""
    if not dados_api or "pedidos" not in dados_api:
        return pd.DataFrame()
    
    pedidos = dados_api["pedidos"]
    
    # Converter para DataFrame
    dados_convertidos = []
    for pedido in pedidos:
        dados_convertidos.append({
            "Número do Pedido": pedido.get("numero_pedido", "Desconhecido"),
            "Data": pd.to_datetime(pedido.get("data")) if pedido.get("data") else pd.NaT,
            "Valor Total Z19-Z24": float(pedido.get("valor_total", 0)),
            "Produto": pedido.get("produto", "Desconhecido"),
            "Quantidade": float(pedido.get("quantidade", 0)),
            "Cidade": pedido.get("cidade", "Desconhecido"),
            "Estado": pedido.get("estado", "Desconhecido"),
            "Cliente": pedido.get("cliente", "Desconhecido"),
            "Telefone": pedido.get("telefone", "Desconhecido")
        })
    
    # Criar DataFrame
    df = pd.DataFrame(dados_convertidos)
    
    # Remover duplicatas baseado no número do pedido e data
    # Isso garante que cada pedido seja contabilizado apenas uma vez
    df = df.drop_duplicates(subset=['Número do Pedido', 'Data'])
    
    return df
 
def verificar_duplicatas(df):
    """Verifica e relata duplicatas no DataFrame"""
    # Verificar duplicatas baseado no número do pedido
    duplicatas = df[df.duplicated(subset=['Número do Pedido'], keep=False)]
    
    if not duplicatas.empty:
        st.warning(f"Foram encontradas {len(duplicatas)} duplicatas!")
        
        # Mostrar as duplicatas
        with st.expander("Ver Duplicatas"):
            st.dataframe(duplicatas[["Número do Pedido", "Data", "Cliente", "Valor Total Z19-Z24"]])
        
        # Estatísticas de duplicatas
        st.caption(f"Total de pedidos: {len(df)} | Pedidos únicos: {len(df.drop_duplicates(subset=['Número do Pedido']))} | Duplicatas: {len(duplicatas)}")
        
        return True
    else:
        st.success("✅ Nenhuma duplicata encontrada!")
        st.caption(f"Total de pedidos: {len(df)} | Todos são únicos")
        return False
 
def limpar_duplicatas(df):
    """Remove duplicatas do DataFrame"""
    df_limpo = df.drop_duplicates(subset=['Número do Pedido', 'Data'])
    
    st.success(f"Removidas {len(df) - len(df_limpo)} duplicatas!")
    st.caption(f"Registros antes: {len(df)} | Registros depois: {len(df_limpo)}")
    
    return df_limpo
 
# Função para normalizar texto (remover acentos e converter para maiúsculas)
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = ''.join(c for c in unicodedata.normalize('NFD', str(text)) if unicodedata.category(c) != 'Mn')
    return text.strip().upper()
 
# Função para encontrar a cidade mais próxima com fuzzy matching considerando o estado
def find_closest_city_with_state(city, state, city_list, municipios_df, estados_df, threshold=70):
    if not city or city == "DESCONHECIDO":
        return None, None, None
    
    normalized_city = normalize_text(city)
    normalized_state = normalize_text(state) if state else None
    
    # Primeiro, tentar encontrar a cidade no estado correto
    if normalized_state:
        # Filtrar cidades do estado específico
        estado_codigo = get_estado_codigo(normalized_state, estados_df)
        if estado_codigo is not None:
            state_cities = municipios_df[municipios_df['codigo_uf'] == estado_codigo]
            state_city_list = state_cities['nome_normalizado'].tolist()
            
            if state_city_list:
                match = process.extractOne(normalized_city, state_city_list, scorer=fuzz.token_sort_ratio)
                if match and match[1] >= threshold:
                    # Encontrar as coordenadas da cidade correspondente
                    matched_city = match[0]
                    city_info = state_cities[state_cities['nome_normalizado'] == matched_city]
                    if not city_info.empty:
                        return matched_city, city_info.iloc[0]['latitude'], city_info.iloc[0]['longitude']
    
    # Se não encontrou no estado correto, tentar encontrar em qualquer estado
    match = process.extractOne(normalized_city, city_list, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        matched_city = match[0]
        city_info = municipios_df[municipios_df['nome_normalizado'] == matched_city]
        if not city_info.empty:
            # Verificar se a cidade encontrada está no mesmo estado que o informado
            if normalized_state:
                estado_codigo = get_estado_codigo(normalized_state, estados_df)
                if estado_codigo is not None and city_info.iloc[0]['codigo_uf'] != estado_codigo:
                    # Se não estiver, não usar esta correspondência
                    return None, None, None
            return matched_city, city_info.iloc[0]['latitude'], city_info.iloc[0]['longitude']
    
    return None, None, None
 
# Função para obter o código do estado a partir da sigla normalizada
def get_estado_codigo(estado_normalizado, estados_df):
    estado_info = estados_df[estados_df['uf_normalizado'] == estado_normalizado]
    if not estado_info.empty:
        return estado_info.iloc[0]['codigo_uf']
    return None
 
# Função para contar dias úteis no período
def contar_dias_uteis(inicio, fim):
    dias = pd.date_range(start=inicio, end=fim, freq='B')
    return len(dias)
 
# Função para determinar a semana do mês com base no intervalo de 26 a 25
def get_week(data, start_date, end_date):
    total_days = (end_date - start_date).days + 1
    if total_days <= 0 or data < start_date or data > end_date:
        return 0
    days_since_start = (data - start_date).days
    week = ((days_since_start * 4) // total_days) + 1 if days_since_start >= 0 else 0
    return min(max(week, 1), 4)
 
# Função para classificar produtos nas categorias especificadas
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
 
# Função para calcular comissões e bônus
def calcular_comissoes_e_bonus(df, inicio_meta, fim_meta):
    """Calcula comissões e bônus com base nas vendas do período"""
    # Filtrar dados para o período da meta
    df_periodo = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)].copy()
    
    # Remover duplicatas para calcular o valor total corretamente
    df_periodo = df_periodo.drop_duplicates(subset=['Número do Pedido'])
    
    # Calcular valor total vendido
    valor_total_vendido = df_periodo["Valor Total Z19-Z24"].sum()
    
    # Classificar produtos por categoria
    df_periodo["Categoria"] = df_periodo["Produto"].apply(classificar_produto)
    
    # Calcular valor por categoria
    valor_kit_ar = df_periodo[df_periodo["Categoria"] == "KITS AR"]["Valor Total Z19-Z24"].sum()
    valor_pecas_avulsas = df_periodo[df_periodo["Categoria"].isin(["PEÇAS AVULSAS", "KITS ROSCA"])]["Valor Total Z19-Z24"].sum()
    
    # Definir percentuais de comissão
    percentual_kit_ar = 0.007  # 0.7%
    percentual_pecas_avulsas = 0.005  # 0.5%
    
    # Calcular comissões
    comissao_kit_ar = 0
    comissao_pecas_avulsas = 0
    
    if valor_total_vendido > 150000:  # Maior que 150 mil
        comissao_kit_ar = valor_kit_ar * percentual_kit_ar
        comissao_pecas_avulsas = valor_pecas_avulsas * percentual_pecas_avulsas
    
    # Calcular bônus
    bonus = 0
    valor_por_bonus = 200  # R$ 200,00 a cada 50 mil vendido
    
    # Calcular quantos bônus de 50 mil foram atingidos
    quantidade_bonus = int(valor_total_vendido // 50000)
    bonus = quantidade_bonus * valor_por_bonus
    
    # Verificar se a meta mensal foi atingida (R$ 600,00 de premiação)
    meta_atingida = valor_total_vendido >= 200000  # Meta de R$ 200.000
    premio_meta = 600 if meta_atingida else 0
    
    # Calcular ganhos totais
    ganhos_totais = comissao_kit_ar + comissao_pecas_avulsas + bonus + premio_meta
    
    # Criar DataFrame com os resultados
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
 
# Função para carregar dados usando a API local
def carregar_dados_api():
    """Carrega dados usando a API local"""
    # Verificar se a API está acessível
    api_ok, api_info = verificar_api_local()
    
    if not api_ok:
        st.error("❌ Não foi possível conectar à API local")
        st.error("Verifique se a API está rodando no seu computador")
        
        if "erro" in api_info:
            st.error(f"Detalhes do erro: {api_info['erro']}")
        
        st.info("""
        ### Como resolver:
        1. Execute o script de inicialização da API:
           ```
           python iniciar_api.py
           ```
        2. Se estiver usando o Streamlit Cloud, configure o ngrok:
           ```
           python iniciar_api_com_ngrok.py
           ```
        3. Copie a URL pública do ngrok e cole no campo "URL da API Local"
        4. Verifique se a pasta 'pedidos' contém seus arquivos Excel
        5. Aguarde alguns segundos e recarregue a página
        """)
        
        # Retornar DataFrame vazio
        return pd.DataFrame(columns=["Número do Pedido", "Data", "Valor Total Z19-Z24", "Produto", "Quantidade", "Cidade", "Estado", "Cliente", "Telefone"])
    
    # Se a API está OK, mostrar status
    st.success("✅ API local conectada com sucesso!")
    st.caption(f"Status: {api_info.get('status', 'desconhecido')} | Arquivos: {api_info.get('total_arquivos', 0)}")
    
    # Carregar dados do cache ou da API
    if "df_dados" not in st.session_state or "ultima_atualizacao" not in st.session_state:
        st.markdown("<div style='display: flex; justify-content: center; background-color: #1A1A2E; padding: 10px;' id='loading-text'>Carregando dados da API local...</div>", unsafe_allow_html=True)
        
        with st.spinner("Obtendo dados da API local..."):
            # Obter todos os pedidos da API
            dados_api = obter_todos_os_pedidos_api()
            
            # Converter para DataFrame
            df = converter_dados_api_para_dataframe(dados_api)
            
            if df.empty:
                st.warning("Nenhum dado foi obtido da API local. Verifique se há arquivos na pasta 'pedidos'.")
                st.session_state.df_dados = pd.DataFrame(columns=["Número do Pedido", "Data", "Valor Total Z19-Z24", "Produto", "Quantidade", "Cidade", "Estado", "Cliente", "Telefone"])
            else:
                st.session_state.df_dados = df
            
            # Registrar hora da atualização
            st.session_state.ultima_atualizacao = dt.now()
            
            st.markdown('<script>document.getElementById("loading-text").style.display = "none";</script>', unsafe_allow_html=True)
    else:
        # Verificar se há novos arquivos
        arquivos = obter_arquivos_api()
        if arquivos:
            # Obter dados atualizados
            dados_api = obter_todos_os_pedidos_api()
            df_atualizado = converter_dados_api_para_dataframe(dados_api)
            
            # Verificar se houve mudanças
            if len(df_atualizado) != len(st.session_state.df_dados):
                st.session_state.df_dados = df_atualizado
                st.session_state.ultima_atualizacao = dt.now()
                st.experimental_rerun()
    
    # Mostrar hora da última atualização
    if "ultima_atualizacao" in st.session_state:
        st.caption(f"Última atualização: {st.session_state.ultima_atualizacao.strftime('%d/%m/%Y %H:%M:%S')}")
    
    return st.session_state.df_dados
 
# Função para identificar lojistas a recuperar
def identificar_lojistas_recuperar(df):
    # Calcular o número de pedidos por cliente
    pedidos_por_cliente = df.groupby('Cliente').size().reset_index(name='num_pedidos')
    
    # Encontrar a data da última compra por cliente
    ultima_compra = df.groupby('Cliente')['Data'].max().reset_index(name='ultima_compra')
    
    # Combinar as informações
    clientes_info = pd.merge(pedidos_por_cliente, ultima_compra, on='Cliente')
    
    # Calcular meses desde a última compra
    hoje = dt.now()
    clientes_info['meses_sem_comprar'] = (hoje - clientes_info['ultima_compra']).dt.days / 30
    
    # Filtrar clientes com mais de 3 pedidos e mais de 3 meses sem comprar
    lojistas_recuperar = clientes_info[
        (clientes_info['num_pedidos'] > 3) & 
        (clientes_info['meses_sem_comprar'] > 3)
    ]
    
    # Juntar com os dados originais para obter informações completas
    if not lojistas_recuperar.empty:
        # Obter a última ocorrência de cada cliente para ter as informações mais recentes
        lojistas_completos = df.sort_values('Data').drop_duplicates(subset=['Cliente'], keep='last')
        lojistas_recuperar = pd.merge(
            lojistas_recuperar[['Cliente', 'num_pedidos', 'ultima_compra', 'meses_sem_comprar']], 
            lojistas_completos, 
            on='Cliente'
        )
        return lojistas_recuperar
    
    return pd.DataFrame()
 
# Função para gerar tabela de pedidos da meta atual
def gerar_tabela_pedidos_meta_atual(df, inicio_meta, fim_meta):
    """Gera tabela de pedidos para o período da meta especificado"""
    df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)].copy()
    
    if df_meta.empty:
        return pd.DataFrame()
    
    # Agrupar por número do pedido para garantir que cada pedido apareça apenas uma vez
    # Mantendo a primeira ocorrência de cada pedido
    df_meta = df_meta.drop_duplicates(subset=['Número do Pedido'], keep='first')
    
    # Selecionar as colunas desejadas
    tabela = df_meta[["Data", "Número do Pedido", "Cliente", "Valor Total Z19-Z24"]].copy()
    tabela.columns = ["Data do Pedido", "Número do Pedido", "Nome do Cliente", "Valor do Pedido"]
    
    # Formatar a data para dd/mm/aaaa
    tabela["Data do Pedido"] = tabela["Data do Pedido"].dt.strftime("%d/%m/%Y")
    
    # Ordenar por data
    tabela = tabela.sort_values("Data do Pedido")
    
    return tabela
 
# Diretório dos arquivos
diretorio_arquivos = resource_path("pedidos")
 
# Carregar arquivos de estados e municípios
estados_df = pd.read_csv(estados_csv_path)
municipios_df = pd.read_csv(municipios_csv_path)
 
# Preparar dados de municípios para busca eficiente
municipios_df["nome_normalizado"] = municipios_df["nome"].apply(normalize_text)
city_list = municipios_df["nome_normalizado"].tolist()
 
# Normalizar estados para matching
estados_df["uf_normalizado"] = estados_df["uf"].apply(normalize_text)
 
# Streamlit config
st.set_page_config(layout="wide", page_title="Dashboard de Vendas")
 
# Estilo elegante e profissional
st.markdown("""
    <style>
        body { background: linear-gradient(135deg, #1A1A2E, #16213E); color: #E0E0E0; font-family: 'Helvetica Neue', Arial, sans-serif; margin: 0; padding: 0; height: 100vh; width: 100vw; overflow-x: hidden; }
        .stProgress > div > div > div > div { background: linear-gradient(90deg, #4A90E2, #50E3C2); }
        .stSelectbox, .stMultiselect { background-color: #2A2A3D; border: 1px solid #3A3A52; border-radius: 8px; color: #E0E0E0; box-shadow: 0 2px 4px rgba(0,0,0,0.2); width: 100%; padding: 8px; }
        .stMetric { background: linear-gradient(135deg, #2A2A3D, #1E1E2E); border: 1px solid #3A3A52; border-radius: 8px; padding: 15px; color: #E0E0E0; box-shadow: 0 2px 6px rgba(0,0,0,0.3); text-align: center; width: 100%; }
        .section { padding: 25px; margin-bottom: 25px; border-radius: 10px; background: linear-gradient(135deg, #2A2A3D, #1E1E2E); border: 1px solid #3A3A52; box-shadow: 0 4px 12px rgba(0,0,0,0.3); width: 100%; }
        h1, h2, h3 { color: #4A90E2; font-weight: 500; text-transform: uppercase; letter-spacing: 1px; }
        .stApp { padding: 30px; height: 100%; width: 100%; box-sizing: border-box; }
        .stCaption { color: #B0B0B0; font-size: 0.9em; }
        .css-1aumxhk { width: 100% !important; min-width: 0 !important; }
        .css-1d391kg { width: 100% !important; min-width: 0 !important; }
        .stPlotlyChart { width: 100% !important; height: auto !important; }
        .stSpinner { position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; z-index: 1000; width: auto; height: auto; }
        .stTabs [data-baseweb="tab-list"] { background: linear-gradient(135deg, #2A2A3D, #1E1E2E); border-bottom: 1px solid #3A3A52; padding: 0 10px; display: flex; justify-content: center; }
        .stTabs [data-baseweb="tab"] { background-color: #2A2A3D; color: #E0E0E0; padding: 10px 20px; margin: 0 5px; border: 1px solid #3A3A52; border-bottom: none; border-radius: 5px 5px 0 0; cursor: pointer; transition: background-color 0.3s; }
        .stTabs [data-baseweb="tab"]:hover { background-color: #3A3A52; }
        .stTabs [data-baseweb="tab"][aria-selected="true"] { background-color: #1E1E2E; color: #4A90E2; font-weight: bold; }
        .filter-container { display: flex; gap: 15px; align-items: center; }
        .filter-label { font-weight: 500; color: #4A90E2; margin-right: 10px; }
        /* Centralizar gráficos */
        div[data-testid="stHorizontalBlock"] > div:has(div[data-testid="stPlotlyChart"]) {
            display: flex;
            justify-content: center;
        }
        /* Estilo para filtros no topo do sidebar */
        .filtro-topo {
            background: linear-gradient(135deg, #2A2A3D, #1E1E2E);
            border: 1px solid #3A3A52;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        /* Estilo para destacar os ganhos */
        .ganhos-destaque {
            background: linear-gradient(135deg, #2A2A3D, #1E1E2E);
            border: 1px solid #3A3A52;
            border-radius: 8px;
            padding: 15px;
            margin-top: 20px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
            text-align: center;
        }
        .ganhos-valor {
            font-size: 24px;
            font-weight: bold;
            color: #50E3C2;
            margin-top: 10px;
        }
    </style>
""", unsafe_allow_html=True)
 
# Placeholder para atualizar o conteúdo
placeholder = st.empty()
 
# Adicionar painel de configuração da API no sidebar
st.sidebar.title("Configuração da API")
 
# Obter URL atual
api_url_atual = obter_url_api()
 
# Permitir que o usuário insira uma URL manualmente
api_url_personalizada = st.sidebar.text_input(
    "URL da API Local",
    value=api_url_atual,
    help="Insira a URL pública da sua API local (ex: https://abc123.ngrok.io)"
)
 
# Atualizar a URL da API
API_URL = api_url_personalizada
 
# Botão para testar a conexão
if st.sidebar.button("Testar Conexão"):
    try:
        response = requests.get(f"{API_URL}/api/saude", timeout=5)
        if response.status_code == 200:
            st.sidebar.success("✅ Conexão bem-sucedida!")
            dados = response.json()
            st.sidebar.caption(f"Status: {dados.get('status', 'desconhecido')}")
            st.sidebar.caption(f"Arquivos: {dados.get('total_arquivos', 0)}")
        else:
            st.sidebar.error(f"❌ Erro: Status {response.status_code}")
    except Exception as e:
        st.sidebar.error(f"❌ Erro de conexão: {str(e)}")
 
# Verificar status da API
api_ok, api_info = verificar_api_local()
 
if api_ok:
    # Botão para recarregar dados
    if st.sidebar.button("🔄 Recarregar Dados"):
        # Limpar cache
        if "df_dados" in st.session_state:
            del st.session_state.df_dados
        if "ultima_atualizacao" in st.session_state:
            del st.session_state.ultima_atualizacao
        st.experimental_rerun()
    
    # Botão para limpar duplicatas
    if st.sidebar.button("🧹 Limpar Duplicatas"):
        if "df_dados" in st.session_state:
            st.session_state.df_dados = limpar_duplicatas(st.session_state.df_dados)
            st.experimental_rerun()
    
    # Adicionar botão para verificar arquivos (sem mostrar os nomes)
    if st.sidebar.button("🔍 Verificar Arquivos"):
        arquivos = obter_arquivos_api()
        if arquivos:
            st.sidebar.success(f"Encontrados {len(arquivos)} arquivos")
        else:
            st.sidebar.warning("Nenhum arquivo encontrado")
    
    # Separador visual
    st.sidebar.markdown("---")
    
    # Seção de filtros no topo do sidebar
    st.sidebar.markdown('<div class="filtro-topo">', unsafe_allow_html=True)
    st.sidebar.markdown("### Filtros de Período")
    
    # Carregar dados inicialmente
    df = carregar_dados_api()
    
    if not df.empty:
        # DataFrame principal
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
        df["Valor Total Z19-Z24"] = pd.to_numeric(df["Valor Total Z19-Z24"], errors="coerce")
        df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce")
        df["Período_Mês"] = df["Data"].dt.to_period("M")
        df = df.dropna(subset=["Data"])
        
        # Obter anos disponíveis
        anos_disponiveis = sorted(df["Data"].dt.year.unique())
        
        # Obter mês atual como padrão
        hoje = dt.now()
        mes_atual = hoje.month
        ano_atual = hoje.year
        
        # Criar colunas para o sidebar
        col_ano, col_mes = st.sidebar.columns(2)
        
        # Filtro de ano
        with col_ano:
            ano_selecionado = st.selectbox(
                "Ano", 
                anos_disponiveis, 
                index=len(anos_disponiveis)-1,  # Selecionar o último ano por padrão
                key="ano_selecionado"
            )
        
        # Filtro de mês
        with col_mes:
            # Obter meses disponíveis para o ano selecionado
            if ano_selecionado:
                meses_disponiveis = sorted(df[df["Data"].dt.year == ano_selecionado]["Data"].dt.month.unique())
            else:
                meses_disponiveis = sorted(df["Data"].dt.month.unique())
            
            # Converter números de meses para nomes
            nomes_meses = [calendar.month_name[mes] for mes in meses_disponiveis]
            
            # Encontrar o índice do mês atual
            if mes_atual in meses_disponiveis and ano_selecionado == ano_atual:
                indice_mes = meses_disponiveis.index(mes_atual)
            else:
                indice_mes = 0  # Primeiro mês disponível
            
            mes_selecionado = st.selectbox(
                "Mês", 
                nomes_meses, 
                index=indice_mes,
                key="mes_selecionado"
            )
            
            # Converter nome do mês de volta para número
            mes_selecionado_num = meses_disponiveis[nomes_meses.index(mes_selecionado)]
        
        st.sidebar.markdown('</div>', unsafe_allow_html=True)
        
        # Calcular período da meta com base nos filtros
        if mes_selecionado_num == 1:
            # Janeiro: período de 26/12 do ano anterior a 25/01 do ano atual
            inicio_meta = dt(ano_selecionado - 1, 12, 26).replace(hour=0, minute=0, second=0)
            fim_meta = dt(ano_selecionado, 1, 25).replace(hour=23, minute=59, second=59)
        else:
            # Demais meses: período de 26/mês-1 a 25/mês do ano atual
            inicio_meta = dt(ano_selecionado, mes_selecionado_num - 1, 26).replace(hour=0, minute=0, second=0)
            fim_meta = dt(ano_selecionado, mes_selecionado_num, 25).replace(hour=23, minute=59, second=59)
        
        # Filtrar dados para o período da meta
        df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)]
        
        # Calcular valor total vendido sem duplicatas
        df_meta_sem_duplicatas = df_meta.drop_duplicates(subset=['Número do Pedido'])
        valor_total_vendido = df_meta_sem_duplicatas["Valor Total Z19-Z24"].sum() if not df_meta_sem_duplicatas.empty else 0
        
        # Calcular estatísticas
        total_pedidos = len(df_meta)
        pedidos_unicos = len(df_meta_sem_duplicatas)
        duplicatas = total_pedidos - pedidos_unicos
        
        meta_total = 200_000
        percentual_meta = min(1.0, valor_total_vendido / meta_total)
        valor_restante = max(0, meta_total - valor_total_vendido)
        
        with placeholder.container():
            st.subheader(f"Meta Mensal Período: {inicio_meta.strftime('%d/%m/%Y')} a {fim_meta.strftime('%d/%m/%Y')}")
            st.markdown("<hr style='border: 1px solid #3A3A52;'>", unsafe_allow_html=True)
            
            st.progress(percentual_meta, text=f"Progresso da Meta: {percentual_meta*100:.1f}%")
            st.caption(f"Número de pedidos processados: {total_pedidos} | Pedidos únicos: {pedidos_unicos} | Duplicatas: {duplicatas}")
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Vendido (Z19-Z24)", f"R$ {valor_total_vendido:,.2f}")
            col2.metric("Meta", f"R$ {meta_total:,.2f}")
            col3.metric("Restante", f"R$ {valor_restante:,.2f}")
            
            # Criar abas com a nova aba de cálculo de meta
            tab1, tab2, tab3 = st.tabs(["Desempenho Individual", "Análise de Clientes", "Cálculo de Meta"])
            
            with tab1:
                # Obter períodos de fechamento para o ano selecionado
                periodos_fechamento = sorted(df[df["Data"].dt.year == ano_selecionado]["Data"].dt.to_period("M").apply(lambda x: f"{calendar.month_abbr[x.month]} / {x.year}").unique())
                
                # Encontrar o índice do período selecionado
                periodo_selecionado = f"{calendar.month_abbr[mes_selecionado_num]} / {ano_selecionado}"
                
                # CORREÇÃO: Verificar se o período está na lista e obter o índice corretamente
                try:
                    if periodo_selecionado in periodos_fechamento:
                        indice_periodo = periodos_fechamento.index(periodo_selecionado)
                    else:
                        indice_periodo = 0
                except:
                    indice_periodo = 0
                
                periodo_selecionado_local = st.selectbox(
                    "Selecione o período", 
                    periodos_fechamento, 
                    index=indice_periodo,
                    key="local_periodo"
                )
                
                # Filtrar dados para o período selecionado
                mes_ano = periodo_selecionado_local.split(" / ")
                mes = list(calendar.month_abbr).index(mes_ano[0])
                ano = int(mes_ano[1])
                inicio_periodo_local = dt(ano, mes, 26).replace(hour=0, minute=0, second=0)
                fim_periodo_local = (inicio_periodo_local + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                df_desempenho_local = df[(df["Data"] >= inicio_periodo_local) & (df["Data"] <= fim_periodo_local)].copy()
                
                # Primeiro bloco: Gráficos ocupando todo o espaço
                col_d1_full, = st.columns([4])
                with col_d1_full:
                    # Vendas por Dia
                    vendas_dia = df_desempenho_local.groupby(df_desempenho_local["Data"].dt.date)["Valor Total Z19-Z24"].sum().reset_index()
                    fig_dia = px.bar(vendas_dia, x="Data", y="Valor Total Z19-Z24", template="plotly_dark", color_discrete_sequence=["#4A90E2"])
                    fig_dia.update_layout(xaxis_title="Data", yaxis_title="Valor Total (R$)", font=dict(size=10), margin=dict(l=10, r=10, t=30, b=10))
                    st.plotly_chart(fig_dia, use_container_width=True)
                    
                    # Comparação de Vendas: Ano Atual vs Ano Anterior (por Semana)
                    inicio_atual = dt(ano, mes, 26).replace(hour=0, minute=0, second=0)
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
                    fig_comparacao_ano.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{ano}', line=dict(color='#4A90E2')))
                    fig_comparacao_ano.add_trace(go.Scatter(x=vendas_anterior_week["Período"], y=vendas_anterior_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{ano-1}', line=dict(color='#50E3C2')))
                    fig_comparacao_ano.update_layout(
                        template="plotly_dark",
                        xaxis_title="Semanas",
                        yaxis_title="Valor Total (R$)",
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_comparacao_ano, use_container_width=True)
                    
                    # Comparação de Vendas: Mês Atual vs Mês Anterior (por Semana)
                    if mes > 1:
                        # Comparar com o mês anterior do mesmo ano
                        inicio_mes_anterior = dt(ano, mes - 1, 26).replace(hour=0, minute=0, second=0)
                        fim_mes_anterior = (inicio_mes_anterior + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                        
                        df_mes_anterior = df[(df["Data"] >= inicio_mes_anterior) & (df["Data"] <= fim_mes_anterior)].copy()
                        df_mes_anterior["Semana"] = df_mes_anterior["Data"].apply(lambda x: get_week(x, start_date=inicio_mes_anterior, end_date=fim_mes_anterior))
                        vendas_mes_anterior_week = df_mes_anterior.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                        vendas_mes_anterior_week["Período"] = vendas_mes_anterior_week["Semana"].apply(lambda x: f"Semana {x}")
                        
                        fig_comparacao_mes = go.Figure()
                        fig_comparacao_mes.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[mes]} {ano}', line=dict(color='#4A90E2')))
                        fig_comparacao_mes.add_trace(go.Scatter(x=vendas_mes_anterior_week["Período"], y=vendas_mes_anterior_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[mes-1]} {ano}', line=dict(color='#E94F37')))
                        fig_comparacao_mes.update_layout(
                            template="plotly_dark",
                            xaxis_title="Semanas",
                            yaxis_title="Valor Total (R$)",
                            font=dict(size=10),
                            margin=dict(l=10, r=10, t=30, b=10),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                        )
                        st.plotly_chart(fig_comparacao_mes, use_container_width=True)
                
                # Segundo bloco: Gráficos ocupando todo o espaço
                col_d2_full, = st.columns([4])
                with col_d2_full:
                    # Top 10 Produtos Mais Vendidos com Quantidade
                    df_periodo = df_desempenho_local.copy()
                    
                    # Filtrar apenas produtos com Quantidade > 0 e agrupar corretamente
                    df_periodo = df_periodo[df_periodo["Quantidade"] > 0].copy()
                    df_periodo["Produto"] = df_periodo["Produto"].str.strip().str.upper()
                    top_produtos = df_periodo.groupby("Produto")["Quantidade"].sum().reset_index()
                    top_produtos = top_produtos.sort_values(by="Quantidade", ascending=False).head(10)
                    
                    fig_top_produtos = px.bar(top_produtos, x="Produto", y="Quantidade", 
                                            title=f"Top 10 Produtos Mais Vendidos - {inicio_periodo_local.strftime('%d/%m/%Y')} a {fim_periodo_local.strftime('%d/%m/%Y')}",
                                            template="plotly_dark", color_discrete_sequence=["#4A90E2"])
                    fig_top_produtos.update_layout(
                        xaxis_title="Produtos",
                        yaxis_title="Quantidade Vendida",
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        xaxis_tickangle=-45  # Rotacionar rótulos para melhor legibilidade
                    )
                    st.plotly_chart(fig_top_produtos, use_container_width=True)
                    
                    # Vendas por Categoria de Produto (Pie Chart)
                    df_desempenho_local["Categoria"] = df_desempenho_local["Produto"].apply(classificar_produto)
                    vendas_categoria = df_desempenho_local.groupby("Categoria")["Valor Total Z19-Z24"].sum().reset_index()
                    categorias_completas = pd.DataFrame({"Categoria": ["KITS AR", "KITS ROSCA", "PEÇAS AVULSAS"]})
                    vendas_categoria = pd.merge(categorias_completas, vendas_categoria, on="Categoria", how="left").fillna(0)
                    
                    fig_categoria = px.pie(vendas_categoria, names="Categoria", values="Valor Total Z19-Z24",
                                         title=f"Vendas por Categoria - {inicio_periodo_local.strftime('%d/%m/%Y')} a {fim_periodo_local.strftime('%d/%m/%Y')}",
                                         template="plotly_dark",
                                         color_discrete_sequence=["#50E3C2", "#4A90E2", "#E94F37"])
                    fig_categoria.update_traces(textinfo="percent+label", textposition="inside")
                    fig_categoria.update_layout(
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    st.plotly_chart(fig_categoria, use_container_width=True)
                    
                    # Botão para mostrar tabela de pedidos da meta atual
                    if st.button("Mostrar Tabela de Pedidos da Meta Atual"):
                        tabela_pedidos = gerar_tabela_pedidos_meta_atual(df, inicio_meta, fim_meta)
                        if not tabela_pedidos.empty:
                            st.subheader(f"Tabela de Pedidos da Meta Atual ({inicio_meta.strftime('%d/%m/%Y')} a {fim_meta.strftime('%d/%m/%Y')})")
                            
                            # Verificar duplicatas
                            verificar_duplicatas(tabela_pedidos)
                            
                            # Exibir tabela
                            st.dataframe(tabela_pedidos.style.format({'Valor do Pedido': 'R$ {:,.2f}'}), use_container_width=True)
                            
                            # Mostrar estatísticas
                            total_unico = tabela_pedidos['Valor do Pedido'].sum()
                            st.caption(f"Valor total de pedidos únicos: R$ {total_unico:,.2f}")
                        else:
                            st.warning("Não há pedidos no período da meta atual.")
            
            with tab2:
                # Identificar lojistas a recuperar
                df_lojistas_recuperar = identificar_lojistas_recuperar(df)
                
                # Seção 1: Mapas (dois mapas lado a lado)
                
                # Criar duas colunas para os mapas
                col_mapa1, col_mapa2 = st.columns([1, 1])
                
                # Mapa 1: Todos os clientes
                with col_mapa1:
                    
                    # Mapa com todos os clientes, usando filtros aplicados
                    df_mapa = df.copy()
                    df_mapa["Cidade"] = df_mapa["Cidade"].str.strip()
                    df_mapa["Estado"] = df_mapa["Estado"].str.strip().str.upper()
                    
                    # Aplicar fuzzy matching para corrigir erros de ortografia nas cidades considerando o estado
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
                            # Se não encontrou a cidade, usar coordenadas do estado
                            estado_normalizado = normalize_text(estado)
                            estado_info = estados_df[estados_df["uf_normalizado"] == estado_normalizado]
                            if not estado_info.empty:
                                df_mapa.at[index, "latitude"] = estado_info.iloc[0]["latitude"]
                                df_mapa.at[index, "longitude"] = estado_info.iloc[0]["longitude"]
                            else:
                                # Coordenadas padrão (centro do Brasil)
                                df_mapa.at[index, "latitude"] = -15.7801
                                df_mapa.at[index, "longitude"] = -47.9292
                    
                    # Adicionar Estado_Corrigido diretamente do estado original
                    df_mapa["Estado_Corrigido"] = df_mapa["Estado"]
                    
                    # Garantir que todos os clientes apareçam, sem agrupamento que possa remover duplicatas
                    # Manter apenas a última ocorrência de cada cliente
                    df_mapa = df_mapa.sort_values('Data').drop_duplicates(subset=['Cliente'], keep='last')
                    
                    # Adicionar coluna de Coordenadas Atuais como uma string combinada
                    df_mapa["Coordenadas Atuais"] = df_mapa.apply(lambda row: f"({row['latitude']}, {row['longitude']})", axis=1)
                    
                    # Aplicar deslocamento mínimo para evitar sobreposição, mas garantindo que todos apareçam
                    # Agrupar por cidade corrigida
                    cidades_grupo = df_mapa.groupby("Cidade_Corrigida")
                    
                    # Para cada cidade, aplicar um pequeno deslocamento aleatório para cada cliente
                    np.random.seed(42)  # Para consistência
                    for cidade, grupo in cidades_grupo:
                        indices = grupo.index.tolist()
                        n_clientes = len(indices)
                        
                        # Para cada cliente na cidade, aplicar um pequeno deslocamento aleatório
                        for i, idx in enumerate(indices):
                            # Deslocamento aleatório pequeno (entre -0.002 e 0.002 graus, aproximadamente 200m)
                            deslocamento_lat = np.random.uniform(-0.002, 0.002)
                            deslocamento_lon = np.random.uniform(-0.002, 0.002)
                            
                            # Aplicar deslocamento
                            df_mapa.at[idx, "latitude"] += deslocamento_lat
                            df_mapa.at[idx, "longitude"] += deslocamento_lon
                    
                    # Formatar a última compra para o hover
                    df_mapa["Ultima_Compra"] = df_mapa["Data"].dt.strftime("%d/%m/%Y")
                    
                    # Remover linhas sem coordenadas (agora todas têm coordenadas)
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
                                '<extra></extra>',  # Remove the trace name extra
                                customdata=df_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra"]],
                                marker=dict(size=7, color="#4A90E2", opacity=0.9,),
                                
                            ))
                            fig_mapa.update_layout(
                                map_style="carto-darkmatter",
                                mapbox_style="dark",
                                mapbox=dict(
                                    zoom=3,
                                    center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean())
                                ),
                                uirevision="constant",  # Mantém a interatividade do usuário (incluindo zoom)
                                font=dict(size=10),
                                margin=dict(l=10, r=10, t=30, b=10),
                                title="Localização dos Clientes",
                                height=600
                            )
                            st.plotly_chart(fig_mapa, use_container_width=True, config={'scrollZoom': True})
                            
                            # Editor de dados abaixo do mapa
                            df_tabela = df_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Cidade_Corrigida", "Estado_Corrigido", "Coordenadas Atuais"]].copy()
                            st.data_editor(df_tabela, use_container_width=True)
                            
                            # Adicionar botão para exportar dados dos clientes
                            if st.button("Exportar dados dos clientes"):
                                csv = df_tabela.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    label="Download CSV",
                                    data=csv,
                                    file_name='clientes_com_coordenadas.csv',
                                    mime='text/csv'
                                )
                    else:
                        st.warning("Nenhum dado de localização válido após aplicar os filtros. Verifique os dados ou os arquivos CSV de estados/municípios.")
                
                # Mapa 2: Lojistas a Recuperar
                with col_mapa2:
                
                    if not df_lojistas_recuperar.empty:
                        # Preparar dados para o mapa de lojistas a recuperar
                        df_recuperar_mapa = df_lojistas_recuperar.copy()
                        df_recuperar_mapa["Cidade"] = df_recuperar_mapa["Cidade"].str.strip()
                        df_recuperar_mapa["Estado"] = df_recuperar_mapa["Estado"].str.strip().str.upper()
                        
                        # Aplicar fuzzy matching para corrigir erros de ortografia nas cidades considerando o estado
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
                                # Se não encontrou a cidade, usar coordenadas do estado
                                estado_normalizado = normalize_text(estado)
                                estado_info = estados_df[estados_df["uf_normalizado"] == estado_normalizado]
                                if not estado_info.empty:
                                    df_recuperar_mapa.at[index, "latitude"] = estado_info.iloc[0]["latitude"]
                                    df_recuperar_mapa.at[index, "longitude"] = estado_info.iloc[0]["longitude"]
                                else:
                                    # Coordenadas padrão (centro do Brasil)
                                    df_recuperar_mapa.at[index, "latitude"] = -15.7801
                                    df_recuperar_mapa.at[index, "longitude"] = -47.9292
                        
                        # Adicionar Estado_Corrigido diretamente do estado original
                        df_recuperar_mapa["Estado_Corrigido"] = df_recuperar_mapa["Estado"]
                        
                        # Formatar a última compra para o hover
                        df_recuperar_mapa["Ultima_Compra"] = df_recuperar_mapa["Data"].dt.strftime("%d/%m/%Y")
                        
                        # Remover linhas sem coordenadas (agora todas têm coordenadas)
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
                                    '<extra></extra>',  # Remove the trace name extra
                                    customdata=df_recuperar_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra", "meses_sem_comprar"]],
                                    marker=dict(size=9, color="#FFA500", opacity=0.9,),  # Pontos laranjas
                                    
                                ))
                                fig_recuperar.update_layout(
                                    map_style="carto-darkmatter",
                                    mapbox_style="dark",
                                    mapbox=dict(
                                        zoom=3,
                                        center=dict(lat=df_recuperar_mapa["latitude"].mean(), lon=df_recuperar_mapa["longitude"].mean())
                                    ),
                                    uirevision="constant",  # Mantém a interatividade do usuário (incluindo zoom)
                                    font=dict(size=10),
                                    margin=dict(l=10, r=10, t=30, b=10),
                                    title="Lojistas a Recuperar",
                                    height=600
                                )
                                st.plotly_chart(fig_recuperar, use_container_width=True, config={'scrollZoom': True})
                                
                                # Editor de dados abaixo do mapa
                                df_recuperar_tabela = df_recuperar_mapa[["Cliente", "Telefone", "Cidade", "Estado", "Ultima_Compra", "meses_sem_comprar"]].copy()
                                df_recuperar_tabela.columns = ["Cliente", "Telefone", "Cidade", "Estado", "Última Compra", "Meses sem Comprar"]
                                st.data_editor(df_recuperar_tabela, use_container_width=True)
                                
                                # Adicionar botão para exportar dados dos lojistas a recuperar
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
                
                # Seção 2: Gráficos de pizza (dois gráficos lado a lado)
                st.subheader("Análise de Distribuição Geográfica")
                
                # Mapeamento de estados para regiões
                regioes_dict = {
                    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
                    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
                    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
                    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul',
                    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste'
                }
                
                # Adicionar coluna de região
                df_mapa['Regiao'] = df_mapa['Estado_Corrigido'].map(regioes_dict)
                
                # Criar duas colunas para os gráficos de pizza
                col_pie1, col_pie2 = st.columns([1, 1])
                
                with col_pie1:
                    # Gráfico de pizza por região
                    clientes_regiao = df_mapa['Regiao'].value_counts().reset_index()
                    clientes_regiao.columns = ['Região', 'Número de Clientes']
                    
                    fig_regiao = px.pie(clientes_regiao, names='Região', values='Número de Clientes',
                                       template='plotly_dark',
                                       color_discrete_sequence=['#4A90E2', '#50E3C2', '#E94F37', '#F7DC6F', '#BB8FCE'])
                    fig_regiao.update_traces(textinfo='percent+label', textposition='inside')
                    fig_regiao.update_layout(
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        height=400,
                        autosize=True  # Permitir redimensionamento automático
                    )
                    st.plotly_chart(fig_regiao, use_container_width=True)
                
                with col_pie2:
                    # Gráfico de pizza por estado (top 10)
                    clientes_estado = df_mapa['Estado_Corrigido'].value_counts().reset_index()
                    clientes_estado.columns = ['Estado', 'Número de Clientes']
                    
                    # Pegar apenas os top 10 estados
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
                        autosize=True  # Permitir redimensionamento automático
                    )
                    st.plotly_chart(fig_estado, use_container_width=True)
                
                # Seção 3: Gráfico de barras (ocupando todo o espaço)
                st.subheader("Análise de Lojistas por Valor Total de Compras")
                
                # Obter lista de estados únicos para o filtro
                estados_unicos = sorted(df['Estado'].unique())
                estado_selecionado = st.selectbox("Selecione o estado para análise de lojistas", 
                                                 ["Todos"] + estados_unicos,
                                                 key="estado_lojistas")
                
                # Agrupar dados por cliente e estado, somando o valor total
                df_lojistas = df.groupby(['Cliente', 'Estado'])['Valor Total Z19-Z24'].sum().reset_index()
                
                # Filtrar pelo estado selecionado, se não for "Todos"
                if estado_selecionado != "Todos":
                    df_lojistas_filtrado = df_lojistas[df_lojistas['Estado'] == estado_selecionado]
                    titulo_grafico = f"Top 10 Lojistas - {estado_selecionado}"
                else:
                    df_lojistas_filtrado = df_lojistas
                    titulo_grafico = "Top 10 Lojistas - Todos os Estados"
                
                # Ordenar pelo valor total em ordem decrescente e pegar os top 10
                top_lojistas = df_lojistas_filtrado.sort_values(by='Valor Total Z19-Z24', ascending=False).head(10)
                
                # Criar gráfico de barras
                fig_lojistas = px.bar(top_lojistas, 
                                     x='Cliente', 
                                     y='Valor Total Z19-Z24',
                                     title=titulo_grafico,
                                     template='plotly_dark',
                                     color_discrete_sequence=['#4A90E2'])
                
                fig_lojistas.update_layout(
                    xaxis_title="Lojista",
                    yaxis_title="Valor Total de Compras (R$)",
                    font=dict(size=10),
                    margin=dict(l=10, r=10, t=30, b=10),
                    xaxis_tickangle=-45  # Rotacionar rótulos para melhor legibilidade
                )
                
                # Adicionar rótulos de valor nas barras
                fig_lojistas.update_traces(texttemplate='R$ %{y:,.2f}', textposition='outside')
                
                st.plotly_chart(fig_lojistas, use_container_width=True)
                
                # Seção 4: Tabela (ocupando todo o espaço)
                st.subheader("Dados Detalhados dos Lojistas")
                st.dataframe(top_lojistas.style.format({'Valor Total Z19-Z24': 'R$ {:,.2f}'}), use_container_width=True)
            
            # NOVA ABA: Cálculo de Meta
            with tab3:
                st.subheader(f"Cálculo de Comissões e Bônus - {inicio_meta.strftime('%d/%m/%Y')} a {fim_meta.strftime('%d/%m/%Y')}")
                
                # Calcular comissões e bônus
                resultados, valor_total_vendido, meta_atingida = calcular_comissoes_e_bonus(df, inicio_meta, fim_meta)
                
                # Exibir informações sobre o período
                col_info1, col_info2 = st.columns(2)
                col_info1.metric("Valor Total Vendido", f"R$ {valor_total_vendido:,.2f}")
                col_info2.metric("Meta Mensal Atingida", "Sim" if meta_atingida else "Não")
                
                # Exibir tabela de cálculos
                st.subheader("Detalhamento dos Cálculos")
                st.dataframe(resultados.style.format({'Valor (R$)': 'R$ {:,.2f}'}), use_container_width=True)
                
                # Exibir ganhos estimados com destaque
                st.markdown('<div class="ganhos-destaque">', unsafe_allow_html=True)
                st.markdown("### Ganhos Estimados")
                ganhos_totais = resultados.iloc[-1, 1]  # Último valor da tabela
                st.markdown(f'<div class="ganhos-valor">R$ {ganhos_totais:,.2f}</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Adicionar informações explicativas
                with st.expander("Regras de Cálculo"):
                    st.markdown("""
                    ### Percentuais de Comissão:
                    - **KIT AR**: 0.7% (aplicado se valor total vendido > R$ 150.000,00)
                    - **Peças Avulsas e Kit Rosca**: 0.5% (aplicado se valor total vendido > R$ 150.000,00)
                    
                    ### Bônus:
                    - **Bônus por Volume**: R$ 200,00 a cada R$ 50.000,00 vendido
                    - **Prêmio Meta Mensal**: R$ 600,00 (se meta de R$ 200.000,00 for atingida)
                    """)
            
            if st.button("Verificar Arquivos na Pasta 'pedidos'"):
                arquivos = [f for f in os.listdir(diretorio_arquivos) if f.endswith(".xlsx")]
                st.write("Arquivos detectados:", arquivos if arquivos else "Nenhum arquivo .xlsx encontrado.")
else:
    st.sidebar.info("Execute a API local no seu computador")
    
    # Mostrar instruções
    with st.sidebar.expander("Como configurar"):
        st.markdown("""
        ### Usando Ngrok (Recomendado):
        1. Execute o script com ngrok:
           ```
           python iniciar_api_com_ngrok.py
           ```
        2. Copie a URL pública exibida
        3. Cole no campo "URL da API Local" acima
        4. Clique em "Testar Conexão"
        
        ### Manualmente:
        1. Execute o script normal:
           ```
           python iniciar_api.py
           ```
        2. Configure um serviço de túnel (ngrok, cloudflare tunnel, etc.)
        3. Insira a URL pública no campo acima
        """)
    
    with placeholder.container():
        st.warning("Nenhum dado disponível. Adicione arquivos .xlsx na pasta 'pedidos'.")