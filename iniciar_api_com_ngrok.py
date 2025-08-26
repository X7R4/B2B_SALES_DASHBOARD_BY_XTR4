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
        # Tentar obter o token de várias fontes
        token = None
        
        # 1. Verificar variável de ambiente
        token = os.environ.get("NGROK_AUTH_TOKEN")
        
        # 2. Se não encontrar, verificar se há um arquivo de configuração
        if not token and os.path.exists("ngrok_token.txt"):
            try:
                with open("ngrok_token.txt", "r") as f:
                    token = f.read().strip()
                print("✓ Token lido do arquivo ngrok_token.txt")
            except:
                pass
        
        # 3. Se ainda não encontrar, pedir para o usuário inserir
        if not token:
            print("❌ Token do ngrok não encontrado!")
            print("\nPor favor, configure o token do ngrok:")
            print("1. Crie uma conta em https://ngrok.com/")
            print("2. Copie seu token authtoken")
            print("3. Escolha uma das opções abaixo:")
            
            print("\nOpção A - Configurar como variável de ambiente:")
            print("   - Windows: setx NGROK_AUTH_TOKEN \"seu_token_aqui\"")
            print("   - Linux/Mac: export NGROK_AUTH_TOKEN=\"seu_token_aqui\"")
            print("   - Depois, feche e abra um novo terminal")
            
            print("\nOpção B - Salvar em arquivo:")
            print("   - Execute: python configurar_token_ngrok.py")
            print("   - Siga as instruções para inserir seu token")
            
            print("\nOpção C - Inserir manualmente agora:")
            token = input("   - Digite seu token do ngrok aqui: ").strip()
            
            if token:
                # Salvar o token em um arquivo para uso futuro
                try:
                    with open("ngrok_token.txt", "w") as f:
                        f.write(token)
                    print("✓ Token salvo em ngrok_token.txt para uso futuro")
                except:
                    pass
        
        if not token:
            print("❌ Não foi possível obter o token do ngrok")
            return None
        
        # Configurar o token
        ngrok.set_auth_token(token)
        
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
        
        # Verificar se o arquivo local_api.py existe
        api_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_api.py")
        if not os.path.exists(api_path):
            print(f"❌ Arquivo local_api.py não encontrado em: {api_path}")
            print("Por favor, verifique se o arquivo está no mesmo diretório que este script.")
            return
        
        # Iniciar a API
        try:
            if platform.system() == "Windows":
                subprocess.call([sys.executable, api_path])
            else:
                subprocess.call([sys.executable, api_path])
        except KeyboardInterrupt:
            print("\n👋 API encerrada")
    else:
        print("\n❌ Não foi possível iniciar o ngrok.")
        print("Verifique se o token do ngrok está configurado corretamente.")

if __name__ == "__main__":
    main()