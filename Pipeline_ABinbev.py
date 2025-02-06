#1. Obtenção dos dados (Bronze Layer - Raw Data)
#Passos:

#Consumo da API: Utilizando o endpoint <https://api.openbrewerydb.org/breweries>, podemos fazer chamadas HTTP para coletar os dados das cervejarias.
#Formato de Persistência: A princípio, você pode armazenar os dados em seu formato nativo JSON, pois é o formato que a API retorna.
#Ferramenta de Orquestração: Para consumir a API, podemos usar o Airflow. Ele facilita a automação e agendamento de tarefas, e permite definir retries e tratativas de erro.

import requests
import json
from datetime import datetime

def fetch_breweries():
    url = 'https://api.openbrewerydb.org/breweries'
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        # Salvar o arquivo JSON com timestamp
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        with open(f'bronze_layer_breweries_{timestamp}.json', 'w') as f:
            json.dump(data, f)
    else:
        raise Exception(f"Failed to fetch data, status code: {response.status_code}")

#*********************************************************#*************************************************************************#**********************************************#

#2. Camada de Transformação (Silver Layer - Dados Curados)
#Passos:

#Leitura dos Dados: Uma vez que o arquivo JSON é gerado e armazenado no formato bruto, a tarefa seguinte é transformá-lo para um formato de dados mais eficiente, como Parquet ou Delta Lake. 
#O Parquet é ideal por ser otimizado para leitura e compressão.
#Particionamento por Localização: Vamos transformar os dados de acordo com o local das cervejarias. A coluna city ou state pode ser usada para particionar os dados.
#Transformações adicionais: Se necessário, é possível limpar ou alterar dados, como converter tipos de dados ou padronizar valores.
#Exemplo de código para leitura e transformação com PySpark:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializar Spark
spark = SparkSession.builder.appName("BreweryPipeline").getOrCreate()

# Carregar JSON
df = spark.read.json("bronze_layer_breweries_*.json")

# Transformações
df_cleaned = df.select("name", "type", "city", "state") \
    .filter(col("city").isNotNull())  # Filtrando linhas sem cidade

# Particionar por cidade
df_cleaned.write.partitionBy("city") \
    .parquet("silver_layer_breweries/")

#*********************************************************#*************************************************************************#**********************************************#

#3. Camada de Agregação (Gold Layer - Dados Analíticos)
#Passos:

#Agregação por Tipo e Localização: Criar uma visão agregada que conte a quantidade de cervejarias por tipo e localização.
#Armazenamento em formato otimizado: O formato Parquet ou Delta Lake continua sendo adequado para essa etapa, dado que você vai realizar agregações e deseja alta performance.

# Agregar por tipo de cervejaria e cidade
aggregated_df = df_cleaned.groupBy("type", "city").count()

# Armazenar no formato Gold (Parquet)
aggregated_df.write.parquet("gold_layer_breweries/")

#4. Orquestração e Gerenciamento de Erros
#Passos:

#Airflow é o orquestrador escolhido para agendar o pipeline e gerenciar as dependências entre as etapas.
#Retries e Tratamento de Erros: Podemos configurar retries e tratativas de erros nas tasks do Airflow. Por exemplo, se a API falhar, o Airflow pode tentar novamente antes de alertar sobre a falha.
#configuração de Airflow:

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def fetch_breweries_task():
    # Lógica de obtenção de dados
    fetch_breweries()

def transform_breweries_task():
    # Lógica de transformação
    transform_data()

with DAG('brewery_data_pipeline', start_date=datetime(2025, 2, 4), catchup=False) as dag:
    fetch_task = PythonOperator(task_id='fetch_breweries', python_callable=fetch_breweries_task, retries=3, retry_delay=timedelta(minutes=5))
    transform_task = PythonOperator(task_id='transform_breweries', python_callable=transform_breweries_task)

    fetch_task >> transform_task
#*********************************************************#*************************************************************************#**********************************************#

#5. Monitoramento e Alerting Abordagem para monitoramento:

#Verificação de Qualidade de Dados: Verificação de qualidade, por exemplo, garantindo que o número de registros não seja nulo e que as transformações estejam corretas.
#Alertas em caso de falhas: Airflow permite o envio de notificações (por exemplo, via email ou Slack) quando uma task falha. Isso é útil para monitorar se algo não está funcionando como esperado.
#Alertas em Airflow:


from airflow.utils.dates import days_ago
from airflow.providers.email.operators.email import EmailOperator

failure_alert = EmailOperator(
    task_id='send_failure_email',
    to='your_email@example.com',
    subject='Pipeline Failure Alert',
    html_content="The Brewery Data Pipeline has failed.",
    trigger_rule='all_failed',
    dag=dag
)

#*********************************************************#*************************************************************************#**********************************************#

#6. Repositório GitHub
#Documentação: No repositório GitHub, você deve documentar claramente os passos que a solução segue, os requisitos necessários (como instalar o Airflow ou PySpark), e como executar o pipeline. 

#*********************************************************#*************************************************************************#**********************************************#

#7. Cloud Services (Caso necessário)
#Caso você queira usar AWS S3 para armazenar os dados no data lake, ou AWS EMR ou Databricks para o processamento de dados, inclua instruções detalhadas de configuração no repositório privado ou na documentação, sem publicar as credenciais na solução pública.

"""
Quando se utiliza serviços de nuvem como AWS S3, EMR ou Databricks, é muito importante manter as credenciais seguras e nunca publicá-las em repositórios públicos. Aqui estão algumas sugestões sobre como você pode abordar isso de forma segura na sua documentação:

7.1 Configuração do AWS S3 (para armazenamento de dados no Data Lake):
No caso de usar o AWS S3 para armazenar dados brutos ou transformados, você pode fornecer as seguintes instruções para configuração:

Passos para Configurar o AWS S3:
Criar um Bucket no S3:

No Console AWS, acesse o S3 e crie um novo bucket para armazenar seus dados. Por exemplo, meu-bucket-de-dados.
Configuração das Credenciais de Acesso:

IAM (Identity and Access Management): Crie um usuário no IAM com permissões apropriadas para acessar o bucket S3. O IAM pode ter permissões como AmazonS3FullAccess ou permissões específicas para o seu bucket.
Chaves de Acesso: Gere um par de chaves de acesso (Access Key ID e Secret Access Key) para esse usuário.
Configuração do AWS CLI ou SDK:

Instale o AWS CLI localmente ou dentro do container Docker.

pip install awscli
Configure suas credenciais AWS:

aws configure
Durante a configuração, insira o Access Key ID, Secret Access Key, região (como us-west-2) e o formato de saída.
Acessando o S3 no Código:

Você pode usar a biblioteca boto3 para interagir com o S3 diretamente no seu código. Aqui está um exemplo simples de como salvar arquivos no S3:

import boto3
from botocore.exceptions import NoCredentialsError

def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Upload bem-sucedido para {bucket}/{s3_file}")
    except FileNotFoundError:
        print("Arquivo não encontrado")
    except NoCredentialsError:
        print("Credenciais não encontradas")
Para configurar o PySpark ou Airflow para gravar diretamente no S3, você pode usar a seguinte configuração:

spark = SparkSession.builder \
    .appName("BreweryPipeline") \
    .config("spark.hadoop.fs.s3a.access.key", "<AWS_ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<AWS_SECRET_KEY>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

df.write.parquet("s3a://meu-bucket-de-dados/bronze_layer/")
Importante: Nunca inclua as suas credenciais diretamente no código ou no repositório. Use variáveis de ambiente ou serviços como AWS Secrets Manager para gerenciar as credenciais de maneira mais segura.

7.2 Configuração do AWS EMR (para Processamento de Dados):
Para usar AWS EMR (Elastic MapReduce) no processamento de dados, você pode usar o Apache Spark ou o Hive no cluster EMR.

Passos para Configurar o AWS EMR:
Criar um Cluster EMR:

No Console da AWS, acesse o EMR e crie um novo cluster. Escolha a versão do Spark ou Hadoop conforme necessário para seu pipeline.
Configure os nós de master e worker conforme a necessidade de processamento.
Acessar o Cluster EMR via SSH:

Use o terminal para acessar o cluster via SSH:

ssh -i <path_to_your_key.pem> hadoop@<cluster_master_public_dns>
Configuração do PySpark para EMR:

Você pode enviar o código do PySpark para rodar no cluster EMR via AWS CLI ou configurar o Spark para apontar para o cluster, caso tenha um script local que vai ser executado no EMR.

spark-submit --master yarn --deploy-mode cluster --py-files my_pipeline.py
Leitura/Gravação com S3:

O EMR também pode ler e escrever diretamente para o S3, por isso, configure o acesso ao S3, conforme explicado na seção anterior, e defina o caminho S3 como destino para seus dados.
7.3 Configuração do Databricks (para Processamento de Dados e Analytics):
O Databricks é uma plataforma popular para processamento de dados baseada no Apache Spark, que pode ser integrada diretamente ao AWS.

Passos para Configurar o Databricks:
Criar uma Conta no Databricks:

Registre-se no Databricks e crie um novo Workspace.
Configuração de Credenciais:

Crie uma Databricks Personal Access Token (PAT) na plataforma, que será utilizada para autenticação ao interagir com a API do Databricks.
Use variáveis de ambiente para armazenar o token de forma segura e nunca coloque diretamente no código.
Integração com AWS S3:

Configure o acesso ao S3 no Databricks com as credenciais apropriadas:

spark.conf.set("spark.hadoop.fs.s3a.access.key", "<AWS_ACCESS_KEY>")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "<AWS_SECRET_KEY>")

Rodando o Código no Databricks:

Você pode rodar o código diretamente no Databricks utilizando notebooks interativos.
Alternativamente, suba seus arquivos e jobs no Databricks e execute-os diretamente.

Dicas de Segurança:

Gerenciar Credenciais de Forma Segura:

Use o AWS Secrets Manager ou AWS IAM Roles para evitar a exposição de credenciais no código.
Variáveis de Ambiente: Configure variáveis de ambiente para armazenar as chaves de acesso de maneira segura.
Nunca inclua arquivos como .aws/credentials ou .env no repositório Git. Use arquivos .gitignore para garantir que não sejam enviados.
Controle de Acesso:

Defina permissões específicas para o acesso ao S3 ou qualquer outro serviço AWS, garantindo que cada serviço tenha acesso apenas aos recursos necessários.
Use o IAM Policy para limitar as permissões, evitando permissões excessivas.
"""


#*********************************************************#*************************************************************************#**********************************************#

#CONTAINER

#8. Containerizando com Docker
#Passos principais:

#Dockerfile: Crie um Dockerfile para definir o ambiente necessário para rodar o seu código, incluindo dependências como requests, pyspark, airflow, etc.
#Construção da Imagem Docker: Após criar o Dockerfile, você pode construir a imagem Docker e rodá-la em qualquer servidor que tenha o Docker instalado.
#Execução em Containers: Após a imagem ser criada, você pode rodar o pipeline em um container Docker. Caso queira, pode até configurar múltiplos containers para diferentes partes do pipeline, como um container para o Airflow e outro para a transformação com PySpark.

#Dockerfile
#Containerizar com Python e Airflow:

# Usando uma imagem base com Python
FROM python:3.9-slim

# Definindo o diretório de trabalho
WORKDIR /app

# Instalando as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalando o Airflow
RUN pip install apache-airflow

# Copiando o código da aplicação para dentro do container
COPY . .

# Expondo a porta que o Airflow usa
EXPOSE 8080

# Definindo o comando para rodar o Airflow (ou outra ferramenta de orquestração)
CMD ["airflow", "scheduler"]


#Diretório de trabalho: Definimos o diretório /app onde o código vai estar.
#Comando de execução: O comando que vai rodar o Airflow. Você também pode adicionar o comando do seu pipeline de forma personalizada, caso queira.

#*********************************************************#*************************************************************************#**********************************************#

#9. Arquivo requirements.txt
#No requirements.txt, você deve listar todas as dependências do seu projeto, como:

requests==2.25.1
pyspark==3.1.2
apache-airflow==2.1.2
pandas==1.2.4

#Este arquivo será usado pelo Docker para instalar todas as dependências do projeto.

#*********************************************************#*************************************************************************#**********************************************#

#10. Construção da Imagem Docker
#Com o Dockerfile e o requirements.txt prontos, basta executar os seguintes comandos para construir e rodar o container:

# Construir a imagem Docker
docker build -t brewery-pipeline .

# Rodar o container em background
docker run -d -p 8080:8080 brewery-pipeline

#Isso criará uma imagem com o nome brewery-pipeline e rodará o Airflow na porta 8080.

#*********************************************************#*************************************************************************#**********************************************#

#11. Containerizando o Airflow
#O Airflow pode ser um pouco mais complexo, pois ele envolve vários componentes, como o scheduler, o web server e o worker. Para orquestrar isso com Docker, você pode usar o Docker Compose, que permite criar e gerenciar multi-containers de maneira fácil.

Docker-compose.yml:

version: '3'
services:
  airflow-webserver:
    image: apache/airflow:2.1.2
    environment:
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////root/airflow/airflow.db
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
    networks:
      - airflow_network

  airflow-scheduler:
    image: apache/airflow:2.1.2
    environment:
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////root/airflow/airflow.db
    command: "airflow scheduler"
    networks:
      - airflow_network

  airflow-worker:
    image: apache/airflow:2.1.2
    environment:
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:////root/airflow/airflow.db
    command: "airflow worker"
    networks:
      - airflow_network

networks:
  airflow_network:
    driver: bridge
    
#Webserver: Roda o servidor web do Airflow, acessível na porta 8080.
#Scheduler: Roda o agendador do Airflow, responsável por orquestrar as tarefas.
#Worker: Executa as tarefas agendadas.
#Volume: Usamos o volume para mapear o diretório ./dags local para o diretório de dags do Airflow no container.

#*********************************************************#*************************************************************************#**********************************************#

##12. Kubernetes para Escalabilidade (Opcional)
#Se o seu projeto precisar de escalabilidade, o Kubernetes pode ser uma solução ideal. Ele permite que você gerencie os containers em um cluster, escalando a quantidade de workers ou pods dependendo da carga de trabalho.

#Aqui está uma visão geral de como usar o Kubernetes:

#Deployment: Você cria um Deployment para cada serviço (Airflow Scheduler, Worker, etc.), garantindo que o número de réplicas seja ajustado conforme necessário.
#Pod: Cada container será rodado em um pod dentro do Kubernetes.
#Persistent Storage: Armazene seus dados em volumes persistentes, como o AWS EFS ou Google Persistent Disk, para garantir que os dados do seu pipeline não sejam perdidos quando o pod for reiniciado.

#Vantagens da Containerização
#Portabilidade: O código dentro do container pode ser executado de forma consistente em qualquer lugar, seja no seu ambiente local, servidores ou na nuvem.
#Escalabilidade: Se sua solução for bem modularizada, você pode escalar facilmente as diferentes partes do pipeline.
#Facilidade de Implementação e Manutenção: Containers permitem que você faça atualizações e manutenção sem impactar todo o sistema.
#Consistência: Garantir que todas as dependências estejam na versão correta e funcionando corretamente em todos os ambientes.

