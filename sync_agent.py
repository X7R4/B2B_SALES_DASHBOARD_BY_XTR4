import os
import sys
import shutil
import subprocess
import json  # Importação corrigida
import logging
from pathlib import Path
import schedule
import platform
import requests
import hashlib
import sqlite3
import threading
import time
from datetime import datetime
 
# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_agent.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
 
class SyncAgent:
    def __init__(self, config_file='config.json'):
        self.config = self.load_config(config_file)
        self.client_id = self.config.get('client_id', self.generate_client_id())
        self.pedidos_dir = Path(self.config.get('pedidos_dir', './pedidos'))
        self.api_url = self.config.get('api_url', 'https://seu-app.streamlit.app/api')
        self.api_token = self.config.get('api_token', 'seu_token_secreto')
        self.db_path = Path('sync_agent.db')
        self.init_db()
        
    def load_config(self, config_file):
        """Carregar configuração do arquivo"""
        default_config = {
            "pedidos_dir": "./pedidos",
            "api_url": "https://seu-app.streamlit.app/api",
            "api_token": "seu_token_secreto",
            "sync_interval": 300,  # 5 minutos
            "client_id": ""
        }
        
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    return {**default_config, **config}
            except Exception as e:
                logger.error(f"Erro ao carregar configuração: {e}")
        
        # Criar arquivo de configuração padrão
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def generate_client_id(self):
        """Gerar ID único do cliente"""
        import uuid
        return str(uuid.uuid4())
    
    def init_db(self):
        """Inicializar banco de dados local"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_hashes (
            filename TEXT PRIMARY KEY,
            hash TEXT,
            last_sync TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_file_hash(self, filepath):
        """Calcular hash do arquivo"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()
    
    def get_synced_files(self):
        """Obter lista de arquivos já sincronizados"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute("SELECT filename FROM file_hashes")
        synced_files = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return synced_files
    
    def update_file_hash(self, filename, file_hash):
        """Atualizar hash do arquivo no banco local"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT OR REPLACE INTO file_hashes (filename, hash, last_sync)
        VALUES (?, ?, ?)
        ''', (filename, file_hash, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
    
    def get_remote_files(self):
        """Obter lista de arquivos já sincronizados no servidor"""
        try:
            headers = {
                'Authorization': f'Bearer {self.api_token}'
            }
            
            response = requests.get(
                f"{self.api_url}/synced_files",
                headers=headers,
                params={'client_id': self.client_id},
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json().get('arquivos', [])
            else:
                logger.error(f"Erro ao obter arquivos remotos: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Erro ao obter arquivos remotos: {e}")
            return []
    
    def upload_file(self, filepath):
        """Fazer upload de arquivo para o servidor"""
        try:
            filename = os.path.basename(filepath)
            
            # Verificar se arquivo já foi sincronizado
            remote_files = self.get_remote_files()
            if any(f['nome'] == filename for f in remote_files):
                logger.info(f"Arquivo {filename} já está sincronizado no servidor")
                return True
            
            # Fazer upload
            headers = {
                'Authorization': f'Bearer {self.api_token}'
            }
            
            with open(filepath, 'rb') as f:
                files = {'file': (filename, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
                data = {'client_id': self.client_id}
                
                response = requests.post(
                    f"{self.api_url}/upload",
                    headers=headers,
                    files=files,
                    data=data,
                    timeout=60
                )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Arquivo {filename} sincronizado com sucesso: {result.get('registros', 0)} registros")
                return True
            else:
                logger.error(f"Erro ao sincronizar arquivo {filename}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao fazer upload do arquivo {filepath}: {e}")
            return False
    
    def check_sync_status(self):
        """Verificar status do servidor"""
        try:
            response = requests.get(f"{self.api_url}/status", timeout=10)
            if response.status_code == 200:
                status = response.json()
                logger.info(f"Servidor online: {status.get('total_registros', 0)} registros")
                return True
            else:
                logger.error(f"Servidor retornou status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Erro ao verificar status do servidor: {e}")
            return False
    
    def sync_files(self):
        """Sincronizar todos os arquivos da pasta pedidos"""
        logger.info("Iniciando sincronização de arquivos...")
        
        if not self.pedidos_dir.exists():
            logger.error(f"Diretório de pedidos não encontrado: {self.pedidos_dir}")
            return
        
        # Obter arquivos Excel na pasta
        excel_files = list(self.pedidos_dir.glob("*.xlsx"))
        
        if not excel_files:
            logger.info("Nenhum arquivo Excel encontrado para sincronizar")
            return
        
        # Verificar status do servidor
        if not self.check_sync_status():
            logger.error("Servidor offline, pulando sincronização")
            return
        
        # Sincronizar cada arquivo
        for filepath in excel_files:
            filename = os.path.basename(filepath)
            
            # Calcular hash atual
            current_hash = self.get_file_hash(filepath)
            
            # Obter hash anterior
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            cursor.execute("SELECT hash FROM file_hashes WHERE filename = ?", (filename,))
            result = cursor.fetchone()
            conn.close()
            
            # Verificar se arquivo mudou
            if result and result[0] == current_hash:
                logger.info(f"Arquivo {filename} não mudou, pulando sincronização")
                continue
            
            # Fazer upload
            if self.upload_file(filepath):
                # Atualizar hash local
                self.update_file_hash(filename, current_hash)
        
        logger.info("Sincronização concluída")
    
    def run(self):
        """Executar o agente de sincronização"""
        logger.info("Iniciando agente de sincronização...")
        logger.info(f"ID do cliente: {self.client_id}")
        logger.info(f"Diretório de pedidos: {self.pedidos_dir}")
        logger.info(f"URL da API: {self.api_url}")
        
        # Verificar se diretório existe
        if not self.pedidos_dir.exists():
            logger.info(f"Criando diretório de pedidos: {self.pedidos_dir}")
            self.pedidos_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar agendamento
        schedule.every(self.config.get('sync_interval', 300)).seconds.do(self.sync_files)
        
        # Fazer sincronização inicial
        self.sync_files()
        
        # Manter o agente rodando
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def create_desktop_shortcut(self):
        """Criar atalho na área de trabalho"""
        try:
            desktop = Path.home() / "Desktop"
            
            if platform.system() == "Windows":
                import win32com.client
                
                shortcut_path = desktop / "Agente de Sincronização.lnk"
                shell = win32com.client.Dispatch("WScript.Shell")
                shortcut = shell.CreateShortCut(str(shortcut_path))
                shortcut.Targetpath = sys.executable
                shortcut.Arguments = f'"{__file__}"'
                shortcut.IconLocation = sys.executable
                shortcut.save()
                
            elif platform.system() == "Darwin":
                script_path = desktop / "Agente de Sincronização.command"
                
                with open(script_path, "w") as f:
                    f.write("#!/bin/bash\n")
                    f.write(f'cd "{os.path.dirname(__file__)}"\n')
                    f.write(f'"{sys.executable}" "{__file__}"\n')
                
                os.chmod(script_path, 0o755)
                
            elif platform.system() == "Linux":
                desktop_dir = Path.home() / ".local/share/applications"
                desktop_dir.mkdir(parents=True, exist_ok=True)
                
                desktop_file = desktop_dir / "sync-agent.desktop"
                with open(desktop_file, "w") as f:
                    f.write("[Desktop Entry]\n")
                    f.write("Version=1.0\n")
                    f.write("Type=Application\n")
                    f.write("Name=Agente de Sincronização\n")
                    f.write(f"Exec={sys.executable} {__file__}\n")
                    f.write("Icon=applications-office\n")
                    f.write("Categories=Office;\n")
            
            logger.info("Atalho criado na área de trabalho")
            
        except Exception as e:
            logger.error(f"Erro ao criar atalho: {e}")
 
def main():
    # Verificar se já está rodando
    if len(sys.argv) > 1 and sys.argv[1] == "--install":
        agent = SyncAgent()
        agent.create_desktop_shortcut()
        return
    
    # Iniciar agente
    agent = SyncAgent()
    
    try:
        agent.run()
    except KeyboardInterrupt:
        logger.info("Agente interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
 
if __name__ == "__main__":
    main()