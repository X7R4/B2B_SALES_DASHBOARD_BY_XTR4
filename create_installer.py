import os
import sys
import shutil
import subprocess
import json
import zipfile
from pathlib import Path

def create_installer():
    print("Criando instalador do agente de sincronização...")
    
    # Criar diretório temporário
    temp_dir = Path("temp_installer")
    temp_dir.mkdir(exist_ok=True)
    
    # Copiar arquivos necessários
    files_to_copy = [
        "sync_agent.py",
        "requirements.txt"
    ]
    
    for file in files_to_copy:
        if Path(file).exists():
            shutil.copy(file, temp_dir)
        else:
            print(f"Aviso: Arquivo {file} não encontrado")
    
    # Criar arquivo de configuração padrão
    config = {
        "pedidos_dir": "./pedidos",
        "api_url": "https://seu-app.streamlit.app/api",
        "api_token": "seu_token_secreto",
        "sync_interval": 300,
        "client_id": ""
    }
    
    with open(temp_dir / "config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    # Criar script de instalação
    install_script = temp_dir / "install.py"
    with open(install_script, "w") as f:
        f.write("""
import os
import sys
import subprocess
import json
from pathlib import Path

def install():
    print("Instalando Agente de Sincronização...")
    
    # Instalar dependências
    print("Instalando dependências...")
    subprocess.call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    
    # Criar diretório de instalação
    install_dir = Path.home() / "DashboardVendas"
    install_dir.mkdir(exist_ok=True)
    
    # Copiar arquivos
    current_dir = Path(__file__).parent
    for file in ["sync_agent.py", "config.json"]:
        shutil.copy(current_dir / file, install_dir)
    
    # Criar atalho
    subprocess.call([sys.executable, str(install_dir / "sync_agent.py"), "--install"])
    
    # Criar pasta pedidos
    pedidos_dir = install_dir / "pedidos"
    pedidos_dir.mkdir(exist_ok=True)
    
    print(f"Instalação concluída!")
    print(f"Diretório de instalação: {install_dir}")
    print(f"Coloque os arquivos .xlsx em: {pedidos_dir}")
    print(f"Execute o agente com: python {install_dir / 'sync_agent.py'}")

if __name__ == "__main__":
    install()
""")
    
    # Criar requirements.txt
    requirements = """requests
schedule
pathlib
"""
    
    with open(temp_dir / "requirements.txt", "w") as f:
        f.write(requirements)
    
    # Criar arquivo ZIP
    zip_path = "SyncAgent_Installer.zip"
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in temp_dir.glob("*"):
            zipf.write(file, file.name)
    
    # Limpar diretório temporário
    shutil.rmtree(temp_dir)
    
    print(f"Instalador criado: {zip_path}")
    print("Envie este arquivo para os usuários")

if __name__ == "__main__":
    create_installer()