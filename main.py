import os
from dotenv import load_dotenv
from mods.sql_server import connect_sql_server, run_query
from mods.logger import setup_logger, get_logger

# Define a pasta base (onde o main.py está localizado)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def load_query(file_path: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as file:
        query = file.read()
    return query

def main():
    # Inicializa o sistema de log
    setup_logger()
    logger = get_logger()

    try:
        logger.info('Iniciando o processo de atualização de dados.')

        # Carrega variáveis de ambiente
        load_dotenv()

        # Variáveis
        CONN_STRING = os.getenv('SERVER_CONN_STRING')
        QUERY_PATH = os.path.join(BASE_DIR, 'queries', 'atendimentos.sql')

        logger.info('Carregando query SQL.')
        SQL_QUERY = load_query(QUERY_PATH)

        logger.info('Conectando ao SQL Server.')
        conn = connect_sql_server(CONN_STRING)

        logger.info('Executando a consulta.')
        df = run_query(conn, SQL_QUERY)

        print(df)

    except Exception as e:
        logger.error(f'Erro durante o processo: {e}', exc_info=True)
        raise  # <- Broker for Jenkins

if __name__ == '__main__':
    main()