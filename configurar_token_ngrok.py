import os

def main():
    print("=== Configurador de Token do Ngrok ===\n")
    
    print("Este script irá configurar seu token do ngrok.")
    print("Se você ainda não tem uma conta no ngrok:")
    print("1. Acesse https://ngrok.com/")
    print("2. Crie uma conta gratuita")
    print("3. Copie seu token authtoken do painel\n")
    
    token = input("Digite seu token do ngrok aqui: ").strip()
    
    if not token:
        print("❌ Nenhum token fornecido.")
        return
    
    # Salvar o token em um arquivo
    try:
        with open("ngrok_token.txt", "w") as f:
            f.write(token)
        print("✓ Token salvo com sucesso em ngrok_token.txt")
    except Exception as e:
        print(f"❌ Erro ao salvar token: {e}")
        return
    
    # Perguntar se também quer configurar como variável de ambiente
    configurar_var = input("\nDeseja também configurar como variável de ambiente? (s/n): ").lower()
    
    if configurar_var == 's':
        print("\nConfigurando como variável de ambiente...")
        
        if platform.system() == "Windows":
            # Para Windows
            try:
                subprocess.run(["setx", "NGROK_AUTH_TOKEN", token], check=True)
                print("✓ Variável de ambiente configurada com sucesso!")
                print("⚠️  Feche e abra um novo terminal para que as alterações tenham efeito")
            except:
                print("❌ Erro ao configurar variável de ambiente")
                print("Você pode configurar manualmente com:")
                print("setx NGROK_AUTH_TOKEN", f'"{token}"')
        else:
            # Para Linux/Mac
            print("Adicione a seguinte linha ao seu ~/.bashrc, ~/.zshrc ou ~/.profile:")
            print(f'export NGROK_AUTH_TOKEN="{token}"')
            print("Depois, execute: source ~/.bashrc (ou o arquivo correspondente)")
    
    print("\n✓ Configuração concluída!")
    print("Agora você pode executar: python iniciar_api_com_ngrok.py")

if __name__ == "__main__":
    import platform
    import subprocess
    main()