import os
import subprocess
import sys
import platform
import socket
import time
import threading
from pyngrok import ngrok

def verificar_porta(porta):
    """Verifica se a porta está em uso"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', porta)) == 0

def instalar_dependencias():
    """Instala as dependências necessárias"""
    print("Verificando dependências...")
    
    # Lista de pacotes necessários
    pacotes = ["fastapi", "uvicorn", "pandas", "openpyxl", "python-multipart", "pyngrok"]
    
    for pacote in pacotes:
        try:
            __import__(pacote.replace("-", "_"))
            print(f"✓ {pacote} já está instalado")
        except ImportError:
            print(f"Instalando {pacote}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pacote])
    
    print("✓ Todas as dependências estão instaladas")

def iniciar_ngrok():
    """Inicia o túnel ngrok e retorna a URL pública"""
    print("Iniciando túnel ngrok...")
    
    try:
        # Configurar token do ngrok (substitua pelo seu token)
        # Você pode obter um token gratuito em https://ngrok.com/
        ngrok.set_auth_token("SEU_TOKEN_NGROK")
        
        # Iniciar túnel na porta 8000
        tunnel = ngrok.connect(8000, "http")
        public_url = tunnel.public_url
        
        print(f"✓ Túnel ngrok criado com sucesso!")
        print(f"   URL pública: {public_url}")
        
        # Salvar a URL em um arquivo
        with open("ngrok_url.txt", "w") as f:
            f.write(public_url)
        
        return public_url
    except Exception as e:
        print(f"❌ Erro ao iniciar ngrok: {str(e)}")
        return None

def main():
    print("=== Iniciador da API Local para Dashboard com Ngrok ===\n")
    
    # Verificar se a porta 8000 está em uso
    if verificar_porta(8000):
        print("⚠️  A porta 8000 já está em uso.")
        print("   Isso pode indicar que a API já está rodando.")
        resposta = input("Deseja continuar mesmo assim? (s/n): ").lower()
        if resposta != 's':
            print("Operação cancelada.")
            return
    
    # Instalar dependências
    instalar_dependencias()
    
    # Criar pasta de pedidos se não existir
    os.makedirs("pedidos", exist_ok=True)
    
    # Iniciar ngrok
    public_url = iniciar_ngrok()
    
    if public_url:
        print(f"\n✓ Ambiente configurado com sucesso!")
        print(f"✓ Pasta de pedidos: {os.path.abspath('pedidos')}")
        print(f"✓ URL pública: {public_url}")
        print("\nIniciando API local...")
        print("A API estará disponível em:")
        print(f"   - Localmente: http://localhost:8000")
        print(f"   - Publicamente: {public_url}")
        print("Pressione Ctrl+C para encerrar a API\n")
        
        # Iniciar a API
        try:
            if platform.system() == "Windows":
                subprocess.call([sys.executable, "local_api.py"])
            else:
                subprocess.call([sys.executable, "local_api.py"])
        except KeyboardInterrupt:
            print("\n👋 API encerrada")
    else:
        print("\n❌ Não foi possível iniciar o ngrok.")
        print("Verifique se o token do ngrok está configurado corretamente.")

if __name__ == "__main__":
    main()