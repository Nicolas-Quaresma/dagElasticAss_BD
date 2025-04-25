import json
import re
import traceback
import psycopg2
from psycopg2 import sql
from elasticsearch import Elasticsearch
from openai import OpenAI
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# Variáveis do Airflow para configuração do Elasticsearch e conexão com o PostgreSQL
ELASTIC_USER = Variable.get('elastic_chat_user')
ELASTIC_PASSWORD = Variable.get('elastic_chat_password')
ELASTIC_INDEX_ENDPOINT = Variable.get('elastic_endpoint')

DATABASE = Variable.get('database')
DATABASE_USER = Variable.get('user')
DATABASE_SERVER = Variable.get('host')
DATABASE_PASSWORD = Variable.get('password')
DATABASE_PORT = Variable.get('port')

# Outras configurações, agora obtidas via Airflow
ELASTICSEARCH_INDEX = Variable.get('elasticsearch_index')  # Índice definido no Airflow
API_KEY = Variable.get('openai_api_key')  # Chave da API OpenAI definida no Airflow
ASSISTANT_ID = Variable.get('assistant_id')  # Assistant ID também definido no Airflow

# Função para conectar ao PostgreSQL
def connect_to_postgresql():
    try:
        return psycopg2.connect(
            dbname=DATABASE,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            host=DATABASE_SERVER,
            port=DATABASE_PORT
        )
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

# Função para salvar resposta no PostgreSQL
def save_assistant_response(response_json):
    try:
        conn = connect_to_postgresql()
        if conn is None:
            return "Erro ao conectar ao banco de dados."
        cursor = conn.cursor()
        insert_query = sql.SQL("""
            INSERT INTO econtas.tbl_contrato_doe (
                contract_type, contract_number, addendum_number, municipio,
                contractor_cnpj, contractor_name, contract_object, contract_value,
                term, start_date, end_date, url, publication_date, codigo_identificador
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """)
        data = (
            response_json.get("contract_type", ""),
            response_json.get("contract_number", ""),
            response_json.get("addendum_number", ""),
            response_json.get("municipio", ""),
            response_json.get("contractor_cnpj", ""),
            response_json.get("contractor_name", ""),
            response_json.get("contract_object", ""),
            response_json.get("contract_value", ""),
            response_json.get("term", ""),
            response_json.get("start_date", ""),
            response_json.get("end_date", ""),
            response_json.get("url", ""),
            response_json.get("publication_date", ""),
            response_json.get("codigo_identificador")
        )
        cursor.execute(insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        return "Resposta salva com sucesso!"
    except Exception as e:
        print(f"Erro ao salvar no banco de dados: {e}")
        return "Erro ao salvar no banco de dados."

# Função para obter registro do Elasticsearch
def get_record_from_elasticsearch(record_id):
    try:
        es = Elasticsearch(
            [ELASTIC_INDEX_ENDPOINT],
            http_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
        )
        response = es.get(index=ELASTICSEARCH_INDEX, id=record_id)
        return response["_source"]
    except Exception as e:
        print(f"Erro ao buscar no Elasticsearch: {e}")
        return None

# Função para extrair atos do Elasticsearch
def extract_acts_from_es_record(index_name, document_id):
    try:
        es = Elasticsearch(
            [ELASTIC_INDEX_ENDPOINT],
            http_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
        )
        response = es.get(index=index_name, id=document_id)
        full_text = response['_source'].get('texto_doe', '')
        if not full_text:
            return []
        
        filtered_text = "\n".join(
            line for line in full_text.split("\n")
            if "Amazonas, Quinta-feira" not in line and "Diário Oficial dos Municípios" not in line
        )
        acts = re.split(r'Código Identificador:\s*([A-Z0-9]{9})', filtered_text)
        
        return [
            (f"{acts[i-1].strip()}{acts[i].strip()}", acts[i].strip())
            for i in range(1, len(acts), 2)
        ]
    except Exception as e:
        print(f"Erro ao processar o registro do Elasticsearch: {e}")
        return []

# Função para interagir com o assistente OpenAI
def make_question_with_assistant(prompt):
    try:
        if not API_KEY:
            raise ValueError("A chave da API OpenAI não foi configurada corretamente.")
        
        client = OpenAI(api_key=API_KEY)
        thread = client.beta.threads.create(messages=[{"role": "user", "content": prompt}])
        run = client.beta.threads.runs.create_and_poll(thread_id=thread.id, assistant_id=ASSISTANT_ID)
        messages = list(client.beta.threads.messages.list(thread_id=thread.id, run_id=run.id))
        return messages[0].content[0].text.value, 100
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        return str(e), 1

# Função para verificar se o registro já foi processado
def record_already_processed(record_id):
    try:
        conn = connect_to_postgresql()
        if conn is None:
            return False
        cursor = conn.cursor()
        query = sql.SQL("SELECT COUNT(*) FROM econtas.tbl_contrato_verificacao_doe WHERE id_elastic = %s;")
        cursor.execute(query, (record_id,))
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"Erro ao verificar registro processado: {e}")
        return False

# Função para marcar o registro como processado
def mark_record_as_processed(record_id):
    try:
        conn = connect_to_postgresql()
        if conn is None:
            return "Erro ao conectar ao banco de dados."
        cursor = conn.cursor()
        query = sql.SQL("INSERT INTO econtas.tbl_contrato_verificacao_doe (id_elastic, data_processamento) VALUES (%s, NOW());")
        cursor.execute(query, (record_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return "Registro marcado como processado."
    except Exception as e:
        print(f"Erro ao marcar registro como processado: {e}")
        return "Erro ao marcar registro como processado."

# Função para buscar todos os IDs de registros no Elasticsearch
def get_all_record_ids(index_name):
    try:
        es = Elasticsearch(
            [ELASTIC_INDEX_ENDPOINT],
            http_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
        )
        query_body = {
            "query": {"match_all": {}},
            "size": 1000
        }
        response = es.search(index=index_name, body=query_body)
        hits = response.get("hits", {}).get("hits", [])
        return [hit["_id"] for hit in hits]
    except Exception as e:
        print(f"Erro ao buscar IDs no Elasticsearch: {e}")
        return []

# Função principal para processamento de todos os registros
def process_all_records(**kwargs):
    # Exemplo: acesso ao contexto de execução
    execution_date = kwargs.get('execution_date')
    print(f"Data de execução: {execution_date}")

    record_ids = get_all_record_ids(ELASTICSEARCH_INDEX)
    if not record_ids:
        print("Nenhum registro encontrado no Elasticsearch.")
        return
    for record_id in record_ids:
        print(f"Processando registro {record_id}...")
        process_with_elasticsearch_and_assistant(record_id)

# Função para processamento de um registro
def process_with_elasticsearch_and_assistant(record_id):
    if record_already_processed(record_id):
        print(f"Registro {record_id} já processado, pulando análise.")
        return

    atos = extract_acts_from_es_record(ELASTICSEARCH_INDEX, record_id)
    if not atos:
        print("Nenhum ato identificado no documento.")
        return

    es = Elasticsearch(
        [ELASTIC_INDEX_ENDPOINT],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
    )
    try:
        response = es.get(index=ELASTICSEARCH_INDEX, id=record_id)
        metadados = response["_source"].get("metadados", {})
    except Exception as e:
        print(f"Erro ao obter metadados do Elasticsearch: {e}")
        metadados = {}

    for ato_completo, codigo in atos:
        if "extrato" in ato_completo.lower() and "contrato" in ato_completo.lower():
            ato_completo_com_metadados = f"""
Ato: {ato_completo}
Código: {codigo}
Metadados:
    idPost: {metadados.get('idPost', 'Não disponível')}
    PostDate: {metadados.get('postDate', 'Não disponível')}
    PostTitle: {metadados.get('postTitle', 'Não disponível')}
    URL: {metadados.get('urlPdfDoe', 'Não disponível')}
"""
            response_text, status = make_question_with_assistant(ato_completo_com_metadados.strip())
            if status == 100:
                try:
                    response_json = json.loads(response_text.replace("json", "").strip())
                    print(save_assistant_response(response_json))
                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON: {e}")
            else:
                print("Falha ao processar com o assistente OpenAI.")
    print(mark_record_as_processed(record_id))

# Configuração dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Criação da DAG para rodar diariamente às 9h
with DAG(
    'dagElasticAss_BD',
    default_args=default_args,
    description='Processa registros do Elasticsearch com o assistente OpenAI e salva no PostgreSQL',
    schedule_interval='0 9 * * *',  # Executa todos os dias às 09:00
    catchup=True,
    max_active_runs=1
) as dag:

    process_records = PythonOperator(
        task_id='process_records',
        python_callable=process_all_records,
        provide_context=True  # Agora a função receberá o contexto de execução
    )

    process_records