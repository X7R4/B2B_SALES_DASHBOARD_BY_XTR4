import os
import subprocess
import sys
import time
from pyngrok import ngrok

def main():
    print("=== Iniciador Rápido da API com Ngrok ===\n")
    
    # Verificar se o token do ngrok está configurado
    token = os.environ.get("NGROK_AUTH_TOKEN")
    if not token:
        print("❌ Token do ngrok não encontrado!")
        print("Por favor, configure o token do ngrok:")
        print("1. Crie uma conta em https://ngrok.com/")
        print("2. Copie seu token authtoken")
        print("3. Execute: set NGROK_AUTH_TOKEN=seu_token_aqui")
        print("   (Linux/Mac) ou setx NGROK_AUTH_TOKEN=seu_token_aqui (Windows)")
        return
    
    # Configurar token
    ngrok.set_auth_token(token)
    
    # Iniciar ngrok
    print("Iniciando túnel ngrok...")
    tunnel = ngrok.connect(8000, "http")
    public_url = tunnel.public_url
    
    print(f"✓ API exposta em: {public_url}")
    print("Use esta URL no dashboard do Streamlit Cloud")
    print("Pressione Ctrl+C para encerrar\n")
    
    # Salvar URL
    with open("ngrok_url.txt", "w") as f:
        f.write(public_url)
    
    # Iniciar API
    subprocess.call([sys.executable, "local_api.py"])

if __name__ == "__main__":
    main()