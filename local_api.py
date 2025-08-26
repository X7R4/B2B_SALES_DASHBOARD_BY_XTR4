import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
from datetime import datetime
import logging
 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
app = FastAPI(title="API Local para Dashboard", version="1.0.0")
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
 
PASTA_PEDIDOS = "pedidos"
 
os.makedirs(PASTA_PEDIDOS, exist_ok=True)
 
class ArquivoInfo(BaseModel):
    nome: str
    tamanho: int
    data_modificacao: str
 
class Pedido(BaseModel):
    numero_pedido: Optional[str] = None
    data: Optional[str] = None
    cliente: Optional[str] = None
    valor_total: Optional[float] = None
    produto: Optional[str] = None
    quantidade: Optional[float] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None
    telefone: Optional[str] = None
 
class RespostaArquivos(BaseModel):
    arquivos: List[ArquivoInfo]
    total: int
 
class RespostaPedidos(BaseModel):
    pedidos: List[Pedido]
    total: int
    arquivo: str
 
@app.get("/")
async def root():
    """Rota raiz para verificar se a API está online"""
    return {"mensagem": "API Local para Dashboard está online", "versao": "1.0.0"}
 
@app.get("/api/saude", response_model=dict)
async def verificar_saude():
    """Verifica se a API está funcionando corretamente"""
    try:
        if not os.path.exists(PASTA_PEDIDOS):
            raise HTTPException(status_code=500, detail="Pasta de pedidos não encontrada")
        
        arquivos = [f for f in os.listdir(PASTA_PEDIDOS) if f.endswith(".xlsx")]
        
        return {
            "status": "online",
            "pasta_pedidos": PASTA_PEDIDOS,
            "total_arquivos": len(arquivos),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Erro na verificação de saúde: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/arquivos", response_model=RespostaArquivos)
async def listar_arquivos():
    """Lista todos os arquivos na pasta de pedidos"""
    try:
        arquivos_info = []
        
        for arquivo in os.listdir(PASTA_PEDIDOS):
            if arquivo.endswith(".xlsx"):
                caminho = os.path.join(PASTA_PEDIDOS, arquivo)
                stat = os.stat(caminho)
                
                arquivos_info.append(ArquivoInfo(
                    nome=arquivo,
                    tamanho=stat.st_size,
                    data_modificacao=datetime.fromtimestamp(stat.st_mtime).isoformat()
                ))
        
        return RespostaArquivos(
            arquivos=arquivos_info,
            total=len(arquivos_info)
        )
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/arquivo/{nome_arquivo}", response_model=RespostaPedidos)
async def obter_dados_arquivo(nome_arquivo: str):
    """Retorna os dados de um arquivo específico"""
    try:
        caminho = os.path.join(PASTA_PEDIDOS, nome_arquivo)
        
        if not os.path.exists(caminho):
            raise HTTPException(status_code=404, detail="Arquivo não encontrado")
        
        try:
            df = pd.read_excel(caminho, header=None)
        except Exception as e:
            logger.error(f"Erro ao ler arquivo Excel {nome_arquivo}: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Erro ao ler arquivo Excel: {str(e)}")
        
        pedidos = []
        
        try:
            data_pedido_raw = df.iloc[1, 15]
            try:
                data_pedido = pd.to_datetime(data_pedido_raw, errors="coerce", dayfirst=True)
                if pd.isna(data_pedido):
                    data_pedido = None
                else:
                    data_pedido = data_pedido.strftime("%Y-%m-%d")
            except:
                data_pedido = None
            
            valores_z19_z24 = []
            for i in range(18, 24):
                try:
                    valor = df.iloc[i, 25]
                    if pd.notna(valor):
                        valores_z19_z24.append(float(valor))
                except:
                    pass
            
            valor_total_z = sum(valores_z19_z24) if valores_z19_z24 else 0
            
            try:
                numero_pedido = str(df.iloc[1, 8]) if pd.notna(df.iloc[1, 8]) else "Desconhecido"
            except:
                numero_pedido = "Desconhecido"
            
            try:
                cliente = str(df.iloc[9, 4]) if pd.notna(df.iloc[9, 4]) else "Desconhecido"
            except:
                cliente = "Desconhecido"
            
            try:
                telefone = str(df.iloc[12, 4]) if pd.notna(df.iloc[12, 4]) else "Desconhecido"
            except:
                telefone = "Desconhecido"
            
            try:
                cidade = str(df.iloc[11, 4]) if pd.notna(df.iloc[11, 4]) else "Desconhecido"
            except:
                cidade = "Desconhecido"
            
            try:
                estado = str(df.iloc[11, 17]) if pd.notna(df.iloc[11, 17]) else "Desconhecido"
            except:
                estado = "Desconhecido"
            
            for i in range(18, 24):
                try:
                    quantidade = df.iloc[i, 0]
                    produto = df.iloc[i, 2]
                    
                    if pd.notna(quantidade) and pd.notna(produto) and float(quantidade) > 0:
                        pedidos.append(Pedido(
                            numero_pedido=numero_pedido,
                            data=data_pedido,
                            cliente=cliente,
                            valor_total=valor_total_z,
                            produto=str(produto),
                            quantidade=float(quantidade),
                            cidade=cidade,
                            estado=estado,
                            telefone=telefone
                        ))
                except:
                    continue
            
        except Exception as e:
            logger.error(f"Erro ao processar dados do arquivo {nome_arquivo}: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Erro ao processar dados: {str(e)}")
        
        return RespostaPedidos(
            pedidos=pedidos,
            total=len(pedidos),
            arquivo=nome_arquivo
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao obter dados do arquivo {nome_arquivo}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 
@app.get("/api/todos-pedidos")
async def obter_todos_os_pedidos():
    """Retorna todos os pedidos de todos os arquivos"""
    try:
        todos_os_pedidos = []
        
        for arquivo in os.listdir(PASTA_PEDIDOS):
            if arquivo.endswith(".xlsx"):
                resposta = await obter_dados_arquivo(arquivo)
                todos_os_pedidos.extend(resposta.pedidos)
        
        return {
            "pedidos": [pedido.dict() for pedido in todos_os_pedidos],
            "total": len(todos_os_pedidos)
        }
    except Exception as e:
        logger.error(f"Erro ao obter todos os pedidos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 
@app.exception_handler(Exception)
async def exception_handler(request, exc):
    """Manipulador global de exceções"""
    logger.error(f"Exceção não tratada: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Erro interno do servidor"}
    )
 
if __name__ == "__main__":
    print("Iniciando API Local para Dashboard...")
    print(f"Pasta de pedidos: {os.path.abspath(PASTA_PEDIDOS)}")
    print("Acesse http://localhost:8000/docs para a documentação da API")
    print("Pressione Ctrl+C para encerrar")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")