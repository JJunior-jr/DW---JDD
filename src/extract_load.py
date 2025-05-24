#import das libs
import yfinance as yf
import pandas as pd
from  sqlalchemy import create_engine
from dotenv import load_dotenv
import os



# import das minhas variaveis de ambiente

commodities= ['CL=F', 'GC=F', 'SI=F']

# Carregar as variáveis de ambiente do arquivo .env
DB_HOST= os.getenv('DB_HOST_PROD')
DB_PORT= os.getenv('DB_PORT_PROD')
DB_NAME= os.getenv('DB_NAME_PROD')
DB_USER= os.getenv('DB_USER_PROD')
DB_PASS= os.getenv('DB_PASS_PROD')
DB_SCHEMA= os.getenv('DB_SCHEMA_PROD')

# Criação da engine de conexão com o banco de dados PostgreSQL
DATABASE_URL= f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine= create_engine(DATABASE_URL)


def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'):
    ticker= yf.Ticker('CL=F')
    #aqui retorna um dataframe, porém sem Ticker(nome do ativo) por isso abaixo criamos uma nova coluna chamada simbolo
    dados= ticker.history(period=periodo, interval=intervalo)[['Close']] 
    dados['simbolo']= simbolo
    return dados


def buscar_todos_dados_commodities(commodities):
    todos_dados= []
    for simbolo in commodities:
        dados= buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)



def salvar_no_postgres(df, schema= 'public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)  

if __name__ == '__main__':
    dados_concatnados= buscar_todos_dados_commodities(commodities)
    salvar_no_postgres(dados_concatnados, schema='public')
       
   
   
   
   
   
   
   
   
   
   
    dados= buscar_dados_dos_comodities(simbolo)
# pegar a cotacao doa meus ativos


#concatenar os meus ativos (1..2..3) -> (1)

#salvear no banco de dados 
