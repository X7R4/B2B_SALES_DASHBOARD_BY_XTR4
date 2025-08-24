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

# Função para extrair dados de um arquivo Excel com cache
@st.cache_data
def extrair_dados_arquivo(caminho_arquivo):
    try:
        xl = pd.ExcelFile(caminho_arquivo)
        df = xl.parse(xl.sheet_names[0], header=None)

        data_pedido_raw = df.iloc[1, 15]
        data_pedido = pd.to_datetime(data_pedido_raw, errors="coerce", dayfirst=True)

        if pd.isna(data_pedido) or data_pedido.dayofweek >= 5:
            return None

        valores_z19_z24 = [df.iloc[i, 25] for i in range(18, 24) if pd.notna(df.iloc[i, 25])]
        valor_total_z = sum(pd.to_numeric(valores_z19_z24, errors="coerce"))

        # Extrair quantidades da coluna A19:A24 (índice 0) e descrições da coluna C18:C24 (índice 2)
        quantidades = [df.iloc[i, 0] for i in range(18, 24) if pd.notna(df.iloc[i, 0]) and pd.notna(df.iloc[i, 2]) and df.iloc[i, 0] > 0]
        descricoes = [df.iloc[i, 2] for i in range(18, 24) if pd.notna(df.iloc[i, 0]) and pd.notna(df.iloc[i, 2]) and df.iloc[i, 0] > 0]

        # Extrair cidade e estado (E12 e R12)
        cidade = df.iloc[11, 4] if pd.notna(df.iloc[11, 4]) else "Desconhecido"
        estado = df.iloc[11, 17] if pd.notna(df.iloc[11, 17]) else "Desconhecido"
        cliente = df.iloc[9, 4] if pd.notna(df.iloc[9, 4]) else "Desconhecido"
        telefone = df.iloc[12, 4] if pd.notna(df.iloc[12, 4]) else "Desconhecido"

        # Alinhar quantidades e descrições, usando o menor comprimento para evitar erro de arrays
        min_length = min(len(quantidades), len(descricoes))
        if min_length == 0:
            return [{
                "Data": data_pedido,
                "Valor Total Z19-Z24": valor_total_z,
                "Produto": "Produto Desconhecido",
                "Quantidade": 0,
                "Cidade": cidade,
                "Estado": estado,
                "Cliente": cliente,
                "Telefone": telefone
            }]

        quantidades = [float(q) for q in quantidades[:min_length]]
        descricoes = descricoes[:min_length]
        
        # Retornar uma lista de dicionários, um por cada produto/quantidade
        dados_extraidos = []
        for produto, quantidade in zip(descricoes, quantidades):
            dados_extraidos.append({
                "Data": data_pedido,
                "Valor Total Z19-Z24": valor_total_z / min_length if min_length > 0 else valor_total_z,
                "Produto": produto,
                "Quantidade": quantidade,
                "Cidade": cidade,
                "Estado": estado,
                "Cliente": cliente,
                "Telefone": telefone
            })
        return dados_extraidos

    except Exception as e:
        return None

# Função para carregar dados inicialmente ou atualizar se necessário
def carregar_dados():
    if "df_dados" not in st.session_state:
        st.markdown("<div style='display: flex; justify-content: center; background-color: #1A1A2E; padding: 10px;' id='loading-text'>Carregando dados...</div>", unsafe_allow_html=True)
        with st.spinner("Lendo e processando arquivos Excel pela primeira vez..."):
            processed_files = {}
            dados_extraidos = []
            if not os.path.exists(diretorio_arquivos):
                os.makedirs(diretorio_arquivos)
                st.warning(f"Pasta 'pedidos' criada em {diretorio_arquivos}. Adicione arquivos .xlsx.")
            else:
                for arquivo in os.listdir(diretorio_arquivos):
                    if arquivo.endswith(".xlsx"):
                        caminho = os.path.join(diretorio_arquivos, arquivo)
                        dados = extrair_dados_arquivo(caminho)
                        if dados:
                            dados_extraidos.extend(dados)
                        processed_files[caminho] = os.path.getmtime(caminho)
            if dados_extraidos:
                st.session_state.df_dados = pd.DataFrame(dados_extraidos)
            else:
                st.session_state.df_dados = pd.DataFrame(columns=["Data", "Valor Total Z19-Z24", "Produto", "Quantidade", "Cidade", "Estado", "Cliente", "Telefone"])
            st.session_state.processed_files = processed_files
            st.markdown('<script>document.getElementById("loading-text").style.display = "none";</script>', unsafe_allow_html=True)
    else:
        current_files = {f: os.path.getmtime(os.path.join(diretorio_arquivos, f)) for f in os.listdir(diretorio_arquivos) if f.endswith(".xlsx")}
        if current_files != st.session_state.processed_files:
            with st.spinner("Detectados novos arquivos. Reprocessando..."):
                dados_extraidos = []
                for caminho, mtime in current_files.items():
                    if caminho not in st.session_state.processed_files or st.session_state.processed_files[caminho] != mtime:
                        dados = extrair_dados_arquivo(caminho)
                        if dados:
                            dados_extraidos.extend(dados)
                if dados_extraidos:
                    st.session_state.df_dados = pd.concat([st.session_state.df_dados, pd.DataFrame(dados_extraidos)], ignore_index=True)
                st.session_state.processed_files = current_files

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
    hoje = datetime.now()
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
    </style>
""", unsafe_allow_html=True)

# Placeholder para atualizar o conteúdo
placeholder = st.empty()

# Carregar dados inicialmente
df = carregar_dados()

if not df.empty:
    # DataFrame principal
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df["Valor Total Z19-Z24"] = pd.to_numeric(df["Valor Total Z19-Z24"], errors="coerce")
    df["Quantidade"] = pd.to_numeric(df["Quantidade"], errors="coerce")
    df["Período_Mês"] = df["Data"].dt.to_period("M")
    df = df.dropna(subset=["Data"])

    # Período de fechamento atual: 26/07/2025 a 25/08/2025
    hoje = datetime.now()
    mes_atual = hoje.month
    ano_atual = hoje.year
    inicio_meta = datetime(ano_atual, mes_atual - 1, 26).replace(hour=0, minute=0, second=0)
    fim_meta = (inicio_meta + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)

    df_meta = df[(df["Data"] >= inicio_meta) & (df["Data"] <= fim_meta)]
    valor_total_vendido = df_meta["Valor Total Z19-Z24"].sum() if not df_meta.empty else 0
    meta_total = 200_000
    percentual_meta = min(1.0, valor_total_vendido / meta_total)
    valor_restante = max(0, meta_total - valor_total_vendido)

    with placeholder.container():
        st.subheader(f"Meta Mensal Período: {inicio_meta.strftime('%d/%m/%Y')} a {fim_meta.strftime('%d/%m/%Y')}")
        st.markdown("<hr style='border: 1px solid #3A3A52;'>", unsafe_allow_html=True)

        st.progress(percentual_meta, text=f"Progresso da Meta: {percentual_meta*100:.1f}%")
        st.caption(f"Número de pedidos processados: {len(df_meta)}")

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Vendido (Z19-Z24)", f"R$ {valor_total_vendido:,.2f}")
        col2.metric("Meta", f"R$ {meta_total:,.2f}")
        col3.metric("Restante", f"R$ {valor_restante:,.2f}")

        tab1, tab2 = st.tabs(["Desempenho Individual", "Análise de Clientes"])

        with tab1:
            anos_disponiveis_local = sorted(df["Data"].dt.year.unique())
            ano_selecionado_local = st.selectbox("Selecione o ano", ["Todos"] + list(anos_disponiveis_local), key="local_ano")
            
            if ano_selecionado_local != "Todos":
                df_ano = df[df["Data"].dt.year == ano_selecionado_local]
                periodos_fechamento_local = sorted(df_ano["Data"].dt.to_period("M").apply(lambda x: f"{calendar.month_abbr[x.month]} / {x.year}").unique())
            else:
                periodos_fechamento_local = sorted(df["Data"].dt.to_period("M").apply(lambda x: f"{calendar.month_abbr[x.month]} / {x.year}").unique())
            periodo_selecionado_local = st.selectbox("Selecione o período", ["Todos"] + list(periodos_fechamento_local), key="local_periodo")

            df_desempenho_local = df.copy()
            if ano_selecionado_local != "Todos" and periodo_selecionado_local != "Todos":
                mes_ano = periodo_selecionado_local.split(" / ")
                mes = list(calendar.month_abbr).index(mes_ano[0])
                ano = int(mes_ano[1])
                inicio_periodo_local = datetime(ano, mes, 26).replace(hour=0, minute=0, second=0)
                fim_periodo_local = (inicio_periodo_local + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                df_desempenho_local = df_desempenho_local[(df_desempenho_local["Data"] >= inicio_periodo_local) & (df_desempenho_local["Data"] <= fim_periodo_local)]
            elif ano_selecionado_local != "Todos":
                df_desempenho_local = df_desempenho_local[df_desempenho_local["Data"].dt.year == ano_selecionado_local]

            # Primeiro bloco: Gráficos ocupando todo o espaço
            col_d1_full, = st.columns([4])
            with col_d1_full:
                # Vendas por Dia
                vendas_dia = df_desempenho_local.groupby(df_desempenho_local["Data"].dt.date)["Valor Total Z19-Z24"].sum().reset_index()
                fig_dia = px.bar(vendas_dia, x="Data", y="Valor Total Z19-Z24", template="plotly_dark", color_discrete_sequence=["#4A90E2"])
                fig_dia.update_layout(xaxis_title="Data", yaxis_title="Valor Total (R$)", font=dict(size=10), margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig_dia, use_container_width=True)

                # Comparação de Vendas: 2025 vs 2024 (por Semana)
                if periodo_selecionado_local != "Todos" and ano_selecionado_local != "Todos":
                    mes_ano = periodo_selecionado_local.split(" / ")
                    mes = list(calendar.month_abbr).index(mes_ano[0])
                    ano = int(mes_ano[1])
                    inicio_2025 = datetime(ano, mes, 26).replace(hour=0, minute=0, second=0)
                    fim_2025 = (inicio_2025 + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    inicio_2024 = inicio_2025 - relativedelta(years=1)
                    fim_2024 = fim_2025 - relativedelta(years=1)
                else:
                    inicio_2025 = datetime(ano_atual, mes_atual - 1, 26).replace(hour=0, minute=0, second=0)
                    fim_2025 = (inicio_2025 + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    inicio_2024 = inicio_2025 - relativedelta(years=1)
                    fim_2024 = fim_2025 - relativedelta(years=1)

                df_2025 = df[(df["Data"] >= inicio_2025) & (df["Data"] <= fim_2025)].copy()
                df_2024 = df[(df["Data"] >= inicio_2024) & (df["Data"] <= fim_2024)].copy()

                df_2025["Semana"] = df_2025["Data"].apply(lambda x: get_week(x, start_date=inicio_2025, end_date=fim_2025))
                df_2024["Semana"] = df_2024["Data"].apply(lambda x: get_week(x, start_date=inicio_2024, end_date=fim_2024))

                vendas_2025_week = df_2025.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                vendas_2025_week["Período"] = vendas_2025_week["Semana"].apply(lambda x: f"Semana {x}")
                vendas_2024_week = df_2024.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                vendas_2024_week["Período"] = vendas_2024_week["Semana"].apply(lambda x: f"Semana {x}")

                fig_comparacao_ano = go.Figure()
                fig_comparacao_ano.add_trace(go.Scatter(x=vendas_2025_week["Período"], y=vendas_2025_week["Valor Total Z19-Z24"], mode='lines+markers', name='2025', line=dict(color='#4A90E2')))
                fig_comparacao_ano.add_trace(go.Scatter(x=vendas_2024_week["Período"], y=vendas_2024_week["Valor Total Z19-Z24"], mode='lines+markers', name='2024', line=dict(color='#50E3C2')))
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
                if periodo_selecionado_local != "Todos" and ano_selecionado_local != "Todos":
                    mes_ano = periodo_selecionado_local.split(" / ")
                    mes = list(calendar.month_abbr).index(mes_ano[0])
                    ano = int(mes_ano[1])
                    inicio_atual = datetime(ano, mes, 26).replace(hour=0, minute=0, second=0)
                    fim_atual = (inicio_atual + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    inicio_anterior = inicio_atual - relativedelta(months=1)
                    fim_anterior = fim_atual - relativedelta(months=1)
                else:
                    inicio_atual = datetime(ano_atual, mes_atual - 1, 26).replace(hour=0, minute=0, second=0)
                    fim_atual = (inicio_atual + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    inicio_anterior = inicio_atual - relativedelta(months=1)
                    fim_anterior = fim_atual - relativedelta(months=1)

                df_atual = df[(df["Data"] >= inicio_atual) & (df["Data"] <= fim_atual)].copy()
                df_anterior = df[(df["Data"] >= inicio_anterior) & (df["Data"] <= fim_anterior)].copy()

                df_atual["Semana"] = df_atual["Data"].apply(lambda x: get_week(x, start_date=inicio_atual, end_date=fim_atual))
                df_anterior["Semana"] = df_anterior["Data"].apply(lambda x: get_week(x, start_date=inicio_anterior, end_date=fim_anterior))

                vendas_atual_week = df_atual.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                vendas_atual_week["Período"] = vendas_atual_week["Semana"].apply(lambda x: f"Semana {x}")
                vendas_anterior_week = df_anterior.groupby("Semana")["Valor Total Z19-Z24"].sum().reindex(range(1, 5), fill_value=0).reset_index()
                vendas_anterior_week["Período"] = vendas_anterior_week["Semana"].apply(lambda x: f"Semana {x}")

                fig_comparacao_mes = go.Figure()
                fig_comparacao_mes.add_trace(go.Scatter(x=vendas_atual_week["Período"], y=vendas_atual_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[inicio_atual.month]} {inicio_atual.year}', line=dict(color='#4A90E2')))
                fig_comparacao_mes.add_trace(go.Scatter(x=vendas_anterior_week["Período"], y=vendas_anterior_week["Valor Total Z19-Z24"], mode='lines+markers', name=f'{calendar.month_abbr[inicio_anterior.month]} {inicio_anterior.year}', line=dict(color='#E94F37')))
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
                if periodo_selecionado_local != "Todos" and ano_selecionado_local != "Todos":
                    mes_ano = periodo_selecionado_local.split(" / ")
                    mes = list(calendar.month_abbr).index(mes_ano[0])
                    ano = int(mes_ano[1])
                    inicio_periodo = datetime(ano, mes, 26).replace(hour=0, minute=0, second=0)
                    fim_periodo = (inicio_periodo + relativedelta(months=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                    df_periodo = df_desempenho_local[(df_desempenho_local["Data"] >= inicio_periodo) & (df_desempenho_local["Data"] <= fim_periodo)].copy()
                else:
                    df_periodo = df_desempenho_local[(df_desempenho_local["Data"] >= inicio_meta) & (df_desempenho_local["Data"] <= fim_meta)].copy()

                # Filtrar apenas produtos com Quantidade > 0 e agrupar corretamente
                df_periodo = df_periodo[df_periodo["Quantidade"] > 0].copy()
                df_periodo["Produto"] = df_periodo["Produto"].str.strip().str.upper()
                top_produtos = df_periodo.groupby("Produto")["Quantidade"].sum().reset_index()
                top_produtos = top_produtos.sort_values(by="Quantidade", ascending=False).head(10)

                fig_top_produtos = px.bar(top_produtos, x="Produto", y="Quantidade", 
                                        title=f"Top 10 Produtos Mais Vendidos - {inicio_periodo.strftime('%d/%m/%Y')} a {fim_periodo.strftime('%d/%m/%Y')}",
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
                            '<extra></extra>',  # Remove o trace name extra
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
                                '<extra></extra>',  # Remove o trace name extra
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

        if st.button("Verificar Arquivos na Pasta 'pedidos'"):
            arquivos = [f for f in os.listdir(diretorio_arquivos) if f.endswith(".xlsx")]
            st.write("Arquivos detectados:", arquivos if arquivos else "Nenhum arquivo .xlsx encontrado.")
else:
    with placeholder.container():
        st.warning("Nenhum dado disponível. Adicione arquivos .xlsx na pasta 'pedidos'.")
