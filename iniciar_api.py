import os
import subprocess
import sys
import platform
import socket

def verificar_porta(porta):
    """Verifica se a porta está em uso"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', porta)) == 0

def instalar_dependencias():
    """Instala as dependências necessárias"""
    print("Verificando dependências...")
    
    # Lista de pacotes necessários
    pacotes = ["fastapi", "uvicorn", "pandas", "openpyxl", "python-multipart"]
    
    for pacote in pacotes:
        try:
            __import__(pacote.replace("-", "_"))
            print(f"✓ {pacote} já está instalado")
        except ImportError:
            print(f"Instalando {pacote}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pacote])
    
    print("✓ Todas as dependências estão instaladas")

def main():
    print("=== Iniciador da API Local para Dashboard ===\n")
    
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
    
    print("\n✓ Ambiente configurado com sucesso!")
    print(f"✓ Pasta de pedidos: {os.path.abspath('pedidos')}")
    print("\nIniciando API local...")
    print("A API estará disponível em: http://localhost:8000")
    print("Pressione Ctrl+C para encerrar a API\n")
    
    # Iniciar a API
    try:
        if platform.system() == "Windows":
            subprocess.call([sys.executable, "local_api.py"])
        else:
            subprocess.call([sys.executable, "local_api.py"])
    except KeyboardInterrupt:
        print("\n👋 API encerrada")

if __name__ == "__main__":
    main()