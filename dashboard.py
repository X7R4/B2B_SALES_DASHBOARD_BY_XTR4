import os
# 移除错误的导入: from httplib2 import Credentials
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
from datetime import datetime as dt
import time
from workalendar.america import Brazil
import io
from google.oauth2 import service_account  # 确保使用正确的Credentials类
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests
 
 
# Configurações de autenticação do Google
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
 
# VARIÁVEIS GLOBAIS
last_modified_time = 0
data = None
 
PASTA_ID = "1FfiukpgvZL92AnRcj1LxE6QW195JLSMY"  # ID da pasta do Google Drive
NOME_ARQUIVO = "dados_extraidos.parquet"  # Nome do arquivo Parquet a ser lido
 
# === Autenticação com Google Drive usando st.secrets ===
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
# 修复: 使用正确的Credentials类
creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SCOPES
)
service = build("drive", "v3", credentials=creds)
 
# === Função para buscar arquivo pelo nome dentro de uma pasta ===
def buscar_arquivo_por_nome(nome_arquivo, pasta_id):
    query = f"'{pasta_id}' in parents and name='{nome_arquivo}' and trashed=false"
    resultados = service.files().list(q=query, fields="files(id, name)").execute()
    arquivos = resultados.get("files", [])
    if not arquivos:
        st.error(f"Arquivo '{nome_arquivo}' não encontrado na pasta do Drive.")
        return None
    return arquivos[0]["id"]
 
# === Função para ler Parquet direto do Drive em memória ===
def ler_parquet_drive(file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        # opcional: st.write(f"Download {int(status.progress() * 100)}%")
    fh.seek(0)
    return pd.read_parquet(fh)
 
# === Carregar dados ===
file_id = buscar_arquivo_por_nome(NOME_ARQUIVO, PASTA_ID)
if file_id:
    df = ler_parquet_drive(file_id)
else:
    st.stop()  # para o dashboard se não encontrar o arquivo
 
def check_for_new_file():
    """Verifica manualmente por atualizações no arquivo CSV"""
    global data
    print("Verificando atualizações...")
    # 修复: 使用正确的函数名
    new_data = ler_parquet_drive(buscar_arquivo_por_nome(NOME_ARQUIVO, PASTA_ID))
    if not new_data.empty:
        data = new_data
        st.session_state.df_dados = data.copy()
        st.session_state.ultima_atualizacao = dt.now()
        update_dashboard()
        st.success("✅ Dados atualizados com sucesso!")
    else:
        st.warning("Nenhuma atualização encontrada")
 
def scheduler():
    """Agendador automático de verificação a cada 30 minutos"""
    while True:
        time.sleep(1800)  # 30 minutos em segundos
        print("Verificação automática agendada...")
        check_for_new_file()
 
# ===== FUNÇÕES DE PROCESSAMENTO =====
 
@st.cache_data(ttl=3600)
def process_excel_data(df, file_name):
    """Processa os dados de um arquivo Excel de forma otimizada"""
    pedidos = []
    
    try:
        if df.empty or len(df) < 20 or len(df.columns) < 26:
            return pd.DataFrame()
        
        try:
            data_pedido_raw = df.iloc[1, 15] if len(df) > 1 and len(df.columns) > 15 else None
            if pd.notna(data_pedido_raw):
                data_pedido = pd.to_datetime(data_pedido_raw, errors="coerce", dayfirst=True)
                data_pedido = data_pedido.strftime("%Y-%m-%d") if pd.notna(data_pedido) else None
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
                                    "numero_pedido": numero_pedido,
                                    "data": data_pedido,
                                    "cliente": cliente,
                                    "valor_total": valor_total_z,
                                    "produto": str(produto),
                                    "quantidade": qtd,
                                    "cidade": cidade,
                                    "estado": estado,
                                    "telefone": telefone,
                                    "arquivo_origem": file_name
                                })
                        except:
                            pass
            except:
                continue
        
    except Exception as e:
        st.error(f"Erro ao processar arquivo {file_name}: {e}")
    
    return pd.DataFrame(pedidos)
 
def carregar_dados_google_drive():
    """Carrega dados do Google Drive (agora via Parquet)"""
    if 'df_dados' in st.session_state and 'ultima_atualizacao' in st.session_state:
        if st.session_state.ultima_atualizacao is not None and \
           (dt.now() - st.session_state.ultima_atualizacao).total_seconds() < 1800:
            return st.session_state.df_dados
 
    st.info("🔄 Carregando dados do Parquet consolidado...")
    # 修复: 使用正确的函数名
    file_id = buscar_arquivo_por_nome(NOME_ARQUIVO, PASTA_ID)
    if file_id:
        df_parquet = ler_parquet_drive(file_id)
    else:
        df_parquet = pd.DataFrame()
    
    if not df_parquet.empty:
        st.session_state.df_dados = df_parquet.copy()
        st.session_state.ultima_atualizacao = dt.now()
        st.success(f"✅ Dados carregados do Parquet! {len(df_parquet)} pedidos")
        return df_parquet
    else:
        st.warning("⚠️ Nenhum dado encontrado no Parquet")
        return pd.DataFrame()
 
# ===== FUNÇÕES DE ANÁLISE E VISUALIZAÇÃO =====
 
def verificar_duplicatas(df):
    duplicatas = df[df.duplicated(subset=['numero_pedido'], keep=False)]
    
    if not duplicatas.empty:
        st.warning(f"Foram encontradas {len(duplicatas)} duplicatas!")
        
        with st.expander("Ver Duplicatas"):
            st.dataframe(duplicatas[["numero_pedido", "data", "cliente", "valor_total"]])
        
        st.caption(f"Total de pedidos: {len(df)} | Pedidos únicos: {len(df.drop_duplicates(subset=['numero_pedido']))} | Duplicatas: {len(duplicatas)}")
        return True
    else:
        st.success("✅ Nenhuma duplicata encontrada!")
        st.caption(f"Total de pedidos: {len(df)} | Todos são únicos")
        return False
 
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
    try:
        # Consulta direta no DataFrame (sem DuckDB)
        valor_kit_ar = df[df['produto'].str.contains('KIT', na=False) & ~df['produto'].str.contains('KIT ROSCA', na=False)]['valor_total'].sum()
        valor_pecas_avulsas = df[df['produto'].isin(['PEÇAS AVULSAS', 'KITS ROSCA'])]['valor_total'].sum()
        pedidos_unicos = df['numero_pedido'].nunique()
        
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
        st.error(f"Erro ao calcular comissões: {e}")
        return pd.DataFrame(), 0, False
 
def identificar_lojistas_recuperar(df):
    try:
        # Consulta direta no DataFrame
        lojistas = df.groupby('cliente').agg(
            num_pedidos=('numero_pedido', 'count'),
            ultima_compra=('data', 'max')
        ).reset_index()
        
        # Filtrar lojistas com mais de 3 pedidos e mais de 3 meses sem comprar
        hoje = dt.now()
        lojistas['meses_sem_comprar'] = ((hoje - lojistas['ultima_compra']).dt.days / 30).astype(int)
        lojistas_recuperar = lojistas[(lojistas['num_pedidos'] > 3) & (lojistas['meses_sem_comprar'] > 3)]
        
        if not lojistas_recuperar.empty:
            # Juntar com dados de telefone, cidade e estado
            info_clientes = df.groupby('cliente').agg(
                telefone=('telefone', 'first'),
                cidade=('cidade', 'first'),
                estado=('estado', 'first')
            ).reset_index()
            
            lojistas_recuperar = lojistas_recuperar.merge(info_clientes, on='cliente', how='left')
            lojistas_recuperar = lojistas_recuperar.rename(columns={'ultima_compra': 'data'})
            
            return lojistas_recuperar
        else:
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Erro ao identificar lojistas: {e}")
        return pd.DataFrame()
 
def get_city_list_and_dfs():
    try:
        municipios_url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/municipios.csv"
        estados_url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/csv/estados.csv"
        
        municipios_df = pd.read_csv(municipios_url, dtype={'codigo_ibge': str})
        estados_df = pd.read_csv(estados_url, dtype={'codigo_uf': int})
        
        municipios_df['nome_normalizado'] = municipios_df['nome'].apply(normalize_text)
        estados_df['uf_normalizado'] = estados_df['uf'].apply(normalize_text)
        
        city_list = municipios_df['nome_normalizado'].unique().tolist()
        
        return city_list, municipios_df, estados_df
        
    except Exception as e:
        st.error(f"Erro ao carregar dados de municípios: {e}")
        return [], pd.DataFrame(), pd.DataFrame()
 
def update_dashboard():
    pass  # Placeholder se necessário
 
# ===== CÓDIGO PRINCIPAL DO DASHBOARD =====
 
st.set_page_config(page_title="Dashboard de Vendas - CASTOR", layout="wide")
 
st.markdown("""
<style>
    .filtro-topo {
        background-color: #2A2A2A;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    .ganhos-destaque {
        background-color: #1F1F1F;
        padding: 20px;
        border-radius: 8px;
        text-align: center;
        margin-top: 20px;
    }
    .ganhos-valor {
        font-size: 32px;
        font-weight: bold;
        color: #4A90E2;
    }
    .valor-vermelho {
        color: #FF4444;
        font-size: 24px;
        font-weight: bold;
    }
    .valor-azul {
        color: #4A90E2;
        font-size: 24px;
        font-weight: bold;
    }
    .creditos {
        text-align: center;
        color: #666;
        font-size: 12px;
        margin-top: 20px;
    }
</style>
""", unsafe_allow_html=True)
 
df = carregar_dados_google_drive()
 
if not df.empty:
    anos_disponiveis = sorted(df["data"].dt.year.unique())
    ano_atual = dt.now().year
    mes_atual = dt.now().month
 
    tab1, tab2, tab3 = st.tabs(["📊 Análise Geral", "🗺️ Mapa de Clientes", "🎯 Cálculo de Meta"])
 
    with tab1:
        st.subheader("VERIFICAÇÃO DE DUPLICATAS")
        verificar_duplicatas(df)
        
        st.subheader("ANÁLISE DE VENDAS MENSAIS")
        
        vendas_mensais = df.resample('M', on='data')['valor_total'].sum().reset_index()
        vendas_mensais['mes_ano'] = vendas_mensais['data'].dt.strftime('%Y-%m')
        
        fig_vendas = px.line(vendas_mensais, x='mes_ano', y='valor_total',
                             template='plotly_dark',
                             color_discrete_sequence=['#FF8C00'])
        fig_vendas.update_layout(
            xaxis_title="Mês",
            yaxis_title="Valor Total (R$)",
            font=dict(size=10),
            margin=dict(l=10, r=10, t=30, b=10)
        )
        st.plotly_chart(fig_vendas, use_container_width=True)
 
    with tab2:
        st.subheader("MAPA DE CLIENTES")
        
        with st.spinner("Carregando dados geográficos..."):
            city_list, municipios_df, estados_df = get_city_list_and_dfs()
        
        if not city_list:
            st.error("Não foi possível carregar os dados geográficos.")
        else:
            df_mapa = df.copy()
            df_mapa["cidade"] = df_mapa["cidade"].str.strip()
            df_mapa["estado"] = df_mapa["estado"].str.strip().str.upper()
            
            df_mapa["cidade_corrigida"] = None
            df_mapa["latitude"] = None
            df_mapa["longitude"] = None
            
            for index, row in df_mapa.iterrows():
                cidade = row["cidade"]
                estado = row["estado"]
                cidade_corrigida, lat, lon = find_closest_city_with_state(cidade, estado, city_list, municipios_df, estados_df, threshold=70)
                
                if cidade_corrigida and lat and lon:
                    df_mapa.at[index, "cidade_corrigida"] = cidade_corrigida
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
            
            df_mapa["estado_corrigido"] = df_mapa["estado"]
            df_mapa = df_mapa.dropna(subset=["latitude", "longitude"])
            
            if not df_mapa.empty:
                with st.spinner("Gerando mapa de clientes..."):
                    fig = go.Figure(go.Scattermap(
                        lat=df_mapa["latitude"],
                        lon=df_mapa["longitude"],
                        mode='markers',
                        hovertemplate=
                        '<b>Cliente</b>: %{customdata[0]}<br>'+
                        '<b>Telefone</b>: %{customdata[1]}<br>'+
                        '<b>Cidade</b>: %{customdata[2]}<br>'+
                        '<b>Estado</b>: %{customdata[3]}<br>'+
                        '<extra></extra>',
                        customdata=df_mapa[["cliente", "telefone", "cidade", "estado"]],
                        marker=dict(size=9, color="#FF8C00", opacity=0.9,),
                    ))
                    fig.update_layout(
                        map_style="carto-darkmatter",
                        mapbox_style="dark",
                        mapbox=dict(
                            zoom=3,
                            center=dict(lat=df_mapa["latitude"].mean(), lon=df_mapa["longitude"].mean())
                        ),
                        uirevision="constant",
                        font=dict(size=10),
                        margin=dict(l=10, r=10, t=30, b=10),
                        title="Mapa de Clientes",
                        height=600
                    )
                    st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True})
                    
                    df_tabela = df_mapa[["cliente", "telefone", "cidade", "estado"]].copy()
                    st.data_editor(df_tabela, use_container_width=True)
                    
                    if st.button("Exportar dados de clientes"):
                        csv = df_tabela.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name='clientes_com_coordenadas.csv',
                            mime='text/csv'
                        )
            else:
                st.warning("Nenhum dado de localização válido após aplicar os filtros.")
            
            st.subheader("Lojistas a Recuperar")
            
            df_lojistas_recuperar = identificar_lojistas_recuperar(df)
            
            if not df_lojistas_recuperar.empty:
                df_recuperar_mapa = df_lojistas_recuperar.copy()
                df_recuperar_mapa["cidade"] = df_recuperar_mapa["cidade"].str.strip()
                df_recuperar_mapa["estado"] = df_recuperar_mapa["estado"].str.strip().str.upper()
                
                df_recuperar_mapa["cidade_corrigida"] = None
                df_recuperar_mapa["latitude"] = None
                df_recuperar_mapa["longitude"] = None
                
                for index, row in df_recuperar_mapa.iterrows():
                    cidade = row["cidade"]
                    estado = row["estado"]
                    cidade_corrigida, lat, lon = find_closest_city_with_state(cidade, estado, city_list, municipios_df, estados_df, threshold=70)
                    
                    if cidade_corrigida and lat and lon:
                        df_recuperar_mapa.at[index, "cidade_corrigida"] = cidade_corrigida
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
                
                df_recuperar_mapa["estado_corrigido"] = df_recuperar_mapa["estado"]
                df_recuperar_mapa["ultima_compra"] = df_recuperar_mapa["data"].dt.strftime("%d/%m/%Y")
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
                            customdata=df_recuperar_mapa[["cliente", "telefone", "cidade", "estado", "ultima_compra", "meses_sem_comprar"]],
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
                        st.plotly_chart(fig_recuperar, use_container_width=True, config={'scrollZoom': True})
                        
                        df_recuperar_tabela = df_recuperar_mapa[["cliente", "telefone", "cidade", "estado", "ultima_compra", "meses_sem_comprar"]].copy()
                        st.data_editor(df_recuperar_tabela, use_container_width=True)
                        
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
            
            df_mapa['regiao'] = df_mapa['estado_corrigido'].map(regioes_dict)
            
            col_pie1, col_pie2 = st.columns([1, 1])
            
            with col_pie1:
                clientes_regiao = df_mapa['regiao'].value_counts().reset_index()
                clientes_regiao.columns = ['regiao', 'numero_de_clientes']
                
                fig_regiao = px.pie(clientes_regiao, names='regiao', values='numero_de_clientes',
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
                st.plotly_chart(fig_regiao, use_container_width=True)
            
            with col_pie2:
                clientes_estado = df_mapa['estado_corrigido'].value_counts().reset_index()
                clientes_estado.columns = ['estado', 'numero_de_clientes']
                top_estados = clientes_estado.head(10)
                
                fig_estado = px.pie(top_estados, names='estado', values='numero_de_clientes',
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
                st.plotly_chart(fig_estado, use_container_width=True)
            
            st.subheader("Análise de Lojistas por Valor Total de Compras")
            
            estados_unicos = sorted(df['estado'].unique())
            estado_selecionado = st.selectbox("Selecione o estado para análise de lojistas", 
                                             ["Todos"] + estados_unicos,
                                             key="estado_lojistas")
            
            df_lojistas = df.groupby(['cliente', 'estado'])['valor_total'].sum().reset_index()
            
            if estado_selecionado != "Todos":
                df_lojistas_filtrado = df_lojistas[df_lojistas['estado'] == estado_selecionado]
                titulo_grafico = f"Top 10 Lojistas - {estado_selecionado}"
            else:
                df_lojistas_filtrado = df_lojistas
                titulo_grafico = "Top 10 Lojistas - Todos os Estados"
            
            top_lojistas = df_lojistas_filtrado.sort_values(by='valor_total', ascending=False).head(10)
            
            fig_lojistas = px.bar(top_lojistas, 
                                 x='cliente', 
                                 y='valor_total',
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
            
            st.plotly_chart(fig_lojistas, use_container_width=True)
            
            st.subheader("Dados Detalhados dos Lojistas")
            st.dataframe(top_lojistas.style.format({'valor_total': 'R$ {:,.2f}'}), use_container_width=True)
        
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
                meses_disponiveis = sorted(df[df["data"].dt.year == ano_meta]["data"].dt.month.unique())
            else:
                meses_disponiveis = sorted(df["data"].dt.month.unique())
            
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
        
        try:
            # Filtrar pedidos no período
            df_meta = df[(df["data"] >= inicio_meta) & (df["data"] <= fim_meta)]
            
            valor_total_vendido = df_meta['valor_total'].sum()
            total_pedidos = len(df_meta)
            pedidos_unicos = df_meta['numero_pedido'].nunique()
            duplicatas = total_pedidos - pedidos_unicos
            
        except Exception as e:
            st.error(f"Erro ao consultar dados da meta: {e}")
            valor_total_vendido = 0
            total_pedidos = 0
            pedidos_unicos = 0
            duplicatas = 0
        
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
        st.dataframe(resultados.style.format({'Valor (R$)': 'R$ {:,.2f}'}), use_container_width=True)
        
        st.markdown('<div class="ganhos-destaque">', unsafe_allow_html=True)
        st.markdown("### Ganhos Estimados")
        ganhos_totais = resultados.iloc[-1, 1]
        st.markdown(f'<div class="ganhos-valor">R$ {ganhos_totais:,.2f}</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
 
else:
    st.warning("⚠️ Nenhum dado disponível. Verifique a configuração do Google Drive.")
 
st.markdown('<div class="creditos">developed by @joao_vendascastor</div>', unsafe_allow_html=True)