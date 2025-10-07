import sys
import requests
import base64
import json
import pandas as pd
from datetime import datetime

    
def definir_parametros(language:str, index:str) -> dict:
    """
    Define os parâmetros da URL
    
    Args:
        language (str): Idioma escolhido
        index (str): Indicador das ações negociadas na B3
    Returns:
        (dict): Parametros da API
    """
    parametros = {
        "language": language,
        "index": index
    }

    return parametros


def formar_url(url:str, parametros:dict) -> str:
    """
    Forma a url completa que será utilizada na requisição dos dados

    Args:
        url (str): Caminho da requisição
        parametros (dict): Dicionário com todos os parâmetros
    Return:
        (str): Url final
    """
    params = base64.b64encode(json.dumps(parametros).encode('utf-8')).decode('utf-8')
    url = f"{url}/{params}"

    return url


def consultar_url(url:str) -> requests.Response:
    """
    Faz a requisição HTTP à URL com os parâmetros fornecidos

    Args:
        url (str): URL utilizada na requisição
    Return:
        (requests.Response): Resultado da requisição            
    """
    response = requests.get(url)
    response.raise_for_status()

    return response


def extrair_dados(response: requests.Response) -> pd.DataFrame:
    """
    Extrai os dados retornados pela página

    Args:
        response (requests.Response): Resposta da requisição
    Returns:
        (pd.DataFrame): DataFrame com os dados extraídos
    """
    json_dados = response.json()
    dados = json_dados.get("results", [])
    data = json_dados.get('header').get('date')
    particao = datetime.strptime(data, '%d/%m/%y').strftime('%Y%m%d')

    df = pd.DataFrame(dados)
    df['date'] = data
    df['partition'] = particao

    return df


def consultar_api(url:str, language:str, index:str) -> pd.DataFrame:
    """
    Realiza a consulta completa e retorna os dados em formato DataFrame
    
    Args:
        url (str): Caminho da requisição
        language (str): Idioma escolhido
        index (str): Indicador das ações negociadas na B3
    Returns:
        (pd.DataFrame): dados da B3 extraídos e formatados
    """
    parametros = definir_parametros(language=language, index=index)
    url = formar_url(url=url, parametros=parametros)
    response = consultar_url(url)
    df = extrair_dados(response)

    return df
    
    
def salvar_dados(df:pd.DataFrame, caminho_arquivo:str, colunas_particao:list):
    """
    Salva os dados no s3 em formato .parquet
    
    Args:
        df (pd.DataFrame): DataFrame com os dados
        caminho_arquivo (str): Caminho que os arquivos serão salvos no s3
        colunas_particao (list): Lista com as colunas que serão utilizadas como partição
    """
    df.to_parquet(
        path=caminho_arquivo,
        engine='pyarrow',
        compression='snappy',
        partition_cols=colunas_particao,
        index=False,
        storage_options={"anon": False}
    )
    
    
def main():
    """
    Função principal do script
    """
    # Realiza a consulta aos dados da B3
    df = consultar_api(
        url="https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay",
        language="pt-br",
        index="IBOV"
        )
    
    # Salva os dados no s3 em formato parquet
    salvar_dados(
        df=df, 
        caminho_arquivo="s3://fiap-postech-bigdataarchitecture/data-lake/", 
        colunas_particao=['partition']
        )


if __name__ == '__main__':
    main()