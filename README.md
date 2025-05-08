Integração entre Elasticsearch, OpenAI Assistant e PostgreSQL com Apache Airflow
Este projeto automatiza o processamento de registros armazenados no Elasticsearch utilizando um assistente da OpenAI. O objetivo é extrair informações estruturadas de atos administrativos (como extratos de contratos), interpretar essas informações com auxílio de IA e armazená-las em um banco de dados PostgreSQL. Todo o processo é orquestrado por uma DAG do Apache Airflow.

Funcionalidades:
Acessa registros em um índice do Elasticsearch;

Extrai trechos relevantes (atos) contendo expressões como "extrato contrato";

Envia os atos extraídos para um assistente da OpenAI para análise e estruturação dos dados;

Salva os dados estruturados no banco de dados PostgreSQL;

Marca os registros processados para evitar retrabalho;

Executa o processo automaticamente todos os dias às 9h.


Sobre a DAG
A DAG chamada dagElasticAss_BD é executada uma vez por dia às 9h da manhã. Ela busca até 1000 registros no índice especificado do Elasticsearch, processa cada um com base na presença de atos contendo as palavras "extrato" e "contrato", envia esses atos ao assistente da OpenAI, e armazena a resposta estruturada no PostgreSQL.
