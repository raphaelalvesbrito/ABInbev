**Brewery Data Pipeline**

Este projeto implementa um pipeline de dados para coleta, transformação e análise de informações sobre cervejarias usando a API Open Brewery DB. 
O pipeline é dividido em várias camadas e usa ferramentas como Airflow, PySpark e Docker para orquestração e processamento.

**Camadas do Pipeline:**

**Bronze Layer:** Coleta de dados brutos via API da Open Brewery DB e armazena no formato JSON.

**Silver Layer:** Transformação dos dados, filtrando e particionando-os por cidade e salvando em formato Parquet.

**Gold Layer:** Agregação de dados, contando cervejarias por tipo e cidade, e armazenando no formato Parquet para análise.

**Orquestração com Airflow:** Usando Airflow para agendar e gerenciar as tarefas do pipeline, incluindo a definição de retries e tratamento de falhas.

**Monitoramento e Alerting:** Configuração de alertas e verificações de qualidade de dados.

**Requisitos:**

Python 3.9
Docker
Apache Airflow
PySpark
Iniciar o Pipeline:

**Containerização:**

O projeto é containerizado usando Docker.
Para rodar o pipeline, use o comando:

docker build -t brewery-pipeline .
docker run -d -p 8080:8080 brewery-pipeline

**Estrutura de Diretórios:**

**/bronze_layer_breweries/:** Dados brutos da API.

**/silver_layer_breweries/:** Dados transformados.

**/gold_layer_breweries/:** Dados agregados.

Contribuição:
Sinta-se à vontade para contribuir com melhorias ou novas funcionalidades.
