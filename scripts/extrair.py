import os
import requests
import zipfile
from datetime import datetime

def baixar_e_extrair():
    # Caminho da pasta
    pasta_data = "/home/emd/Documents/data-analysis/data"

    if not os.path.exists(pasta_data):
        os.makedirs(pasta_data)
        print(f"📁 Pasta criada: {pasta_data}")

    ano_atual = datetime.now().year

    # URL Base (sem o ano e o nome do arquivo)
    url_base = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/emendas-parlamentares-apoiamento"

    for ano in range(2020, ano_atual + 1):
        nome_arquivo = f"{ano}_ApoiamentoEmendasParlamentares.zip"
        url_final = f"{url_base}/{nome_arquivo}"
        caminho_zip = os.path.join(pasta_data, nome_arquivo)

        print(f"\n🌐 Tentando baixar ano {ano}...")

        try:
            # Faz a requisição do arquivo
            resposta = requests.get(url_final, stream=True, timeout=30)

            if resposta.status_code == 200:
                # Salva o arquivo ZIP
                with open(caminho_zip, 'wb') as f:
                    for chunk in resposta.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Download concluído: {nome_arquivo}")

                # Extrai o ZIP
                print(f"📦 Extraindo {nome_arquivo}...")
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(pasta_data)

                # Remove o ZIP
                os.remove(caminho_zip)
                print(f"🗑️ Arquivo ZIP removido.")

            elif resposta.status_code == 404:
                print(f"⏭️ Ano {ano} ainda não disponível no servidor (404).")
            else:
                print(f"⚠️ Erro inesperado no ano {ano}: Status {resposta.status_code}")

        except Exception as e:
            print(f"❌ Falha ao processar ano {ano}: {e}")

    print(f"\n✨ Processo finalizado! Seus CSVs estão em: {pasta_data}")

if __name__ == "__main__":
    baixar_e_extrair()
