import os
import sqlite3
import sys
from zipfile import ZipFile

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import pyspark.sql.functions as F
import psutil

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
BASE_DATA_PATH = os.path.join(AIRFLOW_HOME, 'data', 'cnpj_abertos')

sys.path.append('/opt/airflow')
file_url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-11/Empresas0.zip'
zip_file_path = f'{BASE_DATA_PATH}/zip/'
extract_to_path = f'{BASE_DATA_PATH}/bronze/'
file_name = 'estabelecimentos2.csv'
download_from_url = False

parquet_silver_raw_path = f'{BASE_DATA_PATH}/silver_raw'
parquet_silver_clean_path = f'{BASE_DATA_PATH}/silver_clean'
parquet_gold_path = f'{BASE_DATA_PATH}/gold'


# =======================================================
# DECORADOR MONITORÁVEL – (CORRIGIDO!)
# =======================================================


def monitorar_task(func):
    def wrapper(*args, **kwargs):
        from airflow.models import TaskInstance
        ti: TaskInstance = kwargs["ti"]
        task_id = ti.task_id

        # ============ TEMPO ============
        inicio = datetime.now()
        ti.xcom_push(key="start_time", value=inicio.isoformat())

        # Recursos antes da execução
        process = psutil.Process(os.getpid())
        cpu_inicio = process.cpu_percent(interval=None)
        mem_inicio = process.memory_info().rss

        # ============ EXECUTA A FUNÇÃO ============
        resultado = func(*args, **kwargs)

        # ============ TEMPO ============
        fim = datetime.now()
        ti.xcom_push(key="end_time", value=fim.isoformat())

        duracao = (fim - inicio).total_seconds()
        ti.xcom_push(key="duration", value=duracao)

        # ============ RECURSOS ============
        cpu_fim = process.cpu_percent(interval=None)
        mem_fim = process.memory_info().rss

        ti.xcom_push(key="cpu_percent", value=cpu_fim - cpu_inicio)
        ti.xcom_push(key="memory_mb", value=(mem_fim - mem_inicio) / (1024 * 1024))

        # ============ REGISTROS PROCESSADOS ============
        registros = None

        if resultado is not None:
            if isinstance(resultado, dict) and "registros" in resultado:
                registros = resultado["registros"]
            elif isinstance(resultado, (int, float)):
                registros = resultado

        ti.xcom_push(key="registros", value=registros)

        # taxa de processamento
        if registros and duracao > 0:
            throughput = registros / duracao
        else:
            throughput = None
        ti.xcom_push(key="throughput", value=throughput)

        # ============ INFO ESPECÍFICA PARA SPARK ============
        try:
            # se a função retornou dataframe spark
            if hasattr(resultado, "rdd"):
                ti.xcom_push(key="num_partitions", value=resultado.rdd.getNumPartitions())
                ti.xcom_push(key="num_cols", value=len(resultado.columns))
        except:
            pass

        return resultado

    return wrapper


# --------------------------
# FUNÇÃO DE MONITORAMENTO FINAL
# --------------------------
def monitor_pipeline(**context):

    ti = context["ti"]
    tasks = [
        "extract_bronze",
        "analisar_qualidade",
        "limpar_dados",
        "transformar_dados",
        "gerar_estatisticas",
        "load_database"
    ]

    registros = []

    for t in tasks:
        inicio = ti.xcom_pull(task_ids=t, key="start_time")
        fim = ti.xcom_pull(task_ids=t, key="end_time")
        dur = ti.xcom_pull(task_ids=t, key="duration")
        reg = ti.xcom_pull(task_ids=t, key="registros")

        registros.append({
            "task": t,
            "inicio": inicio if inicio else None,
            "fim": fim if fim else None,
            "duracao_seg": round(float(dur), 2) if dur else None,
            "registros_processados": reg if reg is not None else 0
        })

    df = pd.DataFrame(registros)

    # Diretório do monitoramento
    monitor_dir = os.path.join(AIRFLOW_HOME, "data", "cnpj_abertos", "monitoramento")
    os.makedirs(monitor_dir, exist_ok=True)

    # Salva CSV
    csv_path = os.path.join(monitor_dir, "monitoramento_log.csv")
    df.to_csv(csv_path, index=False)

    # Gráfico
    plt.figure(figsize=(10, 4))
    plt.bar(df["task"], df["duracao_seg"])
    plt.title("Duração por Task")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(monitor_dir, "duracao_por_task.png"))

    return {"csv": csv_path}


# =======================================================
# TAREFAS
# =======================================================

@monitorar_task
def cria_estrutura_pastas(**context):
    diretorios = [
        BASE_DATA_PATH,
        zip_file_path,
        extract_to_path
    ]

    for d in diretorios:
        os.makedirs(d, exist_ok=True)
    return {"registros": len(diretorios)}


@monitorar_task
def extract_bronze(**context):
    zip_files = [f for f in os.listdir(zip_file_path) if f.endswith(".zip")]
    if not zip_files:
        print("Nenhum ZIP encontrado. Baixando da URL...")

        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()

            zip_local_path = os.path.join(zip_file_path, os.path.basename(file_url))

            with open(zip_local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            print(f"Download concluído: {zip_local_path}")

            zip_files = [os.path.basename(zip_local_path)]

            zip_full = os.path.join(zip_file_path, zip_files[0])
            with ZipFile(zip_full, 'r') as z:
                z.extractall(extract_to_path)
        except Exception as e:
            print(f"Erro ao baixar arquivo: {e}")
            return {"registros": 0}


    csvs = [f for f in os.listdir(extract_to_path) if f.endswith(".csv")]
    if csvs:
        os.rename(
            os.path.join(extract_to_path, csvs[0]),
            os.path.join(extract_to_path, file_name)
        )
    return {"registros": len(csvs)}


@monitorar_task
def analisar_qualidade(**context):
    spark = SparkSession.builder.appName("qualidade").getOrCreate()

    nome_colunas = [
        'CNPJ_BASICO', 'CNPJ_ORDEM', 'CNPJ_DV', 'IDENTIFICADOR_MATRIZ_FILIAL', 'NOME_FANTASIA',
        'SITUACAO_CADASTRAL', 'DATA_SITUACAO_CADASTRAL', 'MOTIVO_SITUACAO_CADASTRAL',
        'NOME_CIDADE_EXTERIOR', 'PAIS', 'DATA_INICIO_ATIVIDADE', 'CNAE_FISCAL_PRINCIPAL',
        'CNAE_FISCAL_SECUNDARIA', 'TIPO_LOGRADOURO', 'LOGRADOURO', 'NUMERO', 'COMPLEMENTO',
        'BAIRRO', 'CEP', 'UF', 'MUNICIPIO', 'DDD_1', 'TELEFONE_1', 'DDD_2', 'TELEFONE_2',
        'DDD_FAX', 'FAX', 'CORREIO_ELETRONICO', 'SITUACAO_ESPECIAL', 'DATA_SITUACAO_ESPECIAL'
    ]

    df = spark.read.csv(
        f"{extract_to_path}/{file_name}",
        sep=",",
        inferSchema=False,
        header=False,
        encoding="ISO-8859-1"
    ).toDF(*nome_colunas)

    df.write.mode("overwrite").parquet(parquet_silver_raw_path)
    return {"registros": df.count()}


@monitorar_task
def limpar_dados(**context):
    spark = SparkSession.builder.appName("limpar").getOrCreate()
    df = spark.read.parquet(parquet_silver_raw_path)

    df = df.na.drop(subset=["CNPJ_BASICO", "UF", "MUNICIPIO"])
    df = df.fillna({"NOME_FANTASIA": "SEM_NOME",
                    "CORREIO_ELETRONICO": "NAO_INFORMADO",
                    "DDD_1": "NAO_INFORMADO", "DDD_2": "NAO_INFORMADO",
                    "TELEFONE_2": "NAO_INFORMADO",
                    "DDD_FAX": "NAO_INFORMADO",
                    "FAX": "NAO_INFORMADO",
                    "DATA_SITUACAO_ESPECIAL": "NAO_INFORMADO",
                    "COMPLEMENTO": "NAO_INFORMADO",
                    "NOME_CIDADE_EXTERIOR": "NAO_INFORMADO",
                    "PAIS": "BRASIL"
                    })
    df.write.mode("overwrite").parquet(parquet_silver_clean_path)
    return {"registros": df.count()}


@monitorar_task
def transformar_dados(**context):
    spark = SparkSession.builder.appName("transformar").getOrCreate()
    df = spark.read.parquet(parquet_silver_clean_path)

    df = df.withColumn("CNPJ_COMPLETO", concat_ws("", col("CNPJ_BASICO"), col("CNPJ_ORDEM"), col("CNPJ_DV")))
    df = df.withColumn("DATA_ABERTURA", F.to_date(col("DATA_INICIO_ATIVIDADE"), "yyyyMMdd"))
    df = df.withColumn("IDADE_EM_ANOS", F.floor(F.datediff(F.current_date(), col("DATA_ABERTURA")) / 365))
    df = df.withColumn("ATIVO", (col("SITUACAO_CADASTRAL") == "02").cast('boolean'))

    df.write.mode("overwrite").parquet(parquet_gold_path)
    return {"registros": df.count()}


@monitorar_task
def gerar_estatisticas(**context):
    spark = SparkSession.builder.appName("estatisticas").getOrCreate()
    df = spark.read.parquet(parquet_gold_path)

    # Estatísticas gerais
    total_registros = df.count()
    total_colunas = len(df.columns)

    # Tamanho aproximado do dataset em MB
    tamanho_bytes = df.rdd.map(lambda r: len(str(r))).sum()
    tamanho_mb = tamanho_bytes / (1024 * 1024)

    # Contagem de nulos por coluna
    nulos_por_coluna = {
        c: df.filter(df[c].isNull()).count() for c in df.columns
    }

    # Tipos de dados das colunas
    tipos_colunas = {
        c: str(df.schema[c].dataType) for c in df.columns
    }

    # Estatísticas numéricas básicas (se houver colunas numéricas)
    colunas_numericas = [
        c for c, dtype in tipos_colunas.items()
        if "IntegerType" in dtype or "DoubleType" in dtype or "LongType" in dtype or "FloatType" in dtype
    ]

    estatisticas_numericas = {}
    if colunas_numericas:
        descricao = df.select(colunas_numericas).describe().toPandas()
        estatisticas_numericas = descricao.set_index('summary').T.to_dict()

    return {
        "total_registros": total_registros,
        "total_colunas": total_colunas,
        "tamanho_mb_aprox": tamanho_mb,
        "nulos_por_coluna": nulos_por_coluna,
        "tipos_colunas": tipos_colunas,
        "estatisticas_numericas": estatisticas_numericas
    }


@monitorar_task
def load_database(**context):

    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect('data/pipeline.db')

    bronze_file = os.path.join(extract_to_path, file_name)
    if os.path.exists(bronze_file):
        bronze_df = pd.read_csv(bronze_file, sep=",", encoding="ISO-8859-1", header=None)
        bronze_df.to_sql("bronze", conn, if_exists="replace", index=False)

    silver_raw_df = pd.read_parquet(parquet_silver_raw_path)
    silver_raw_df.to_sql("silver_raw", conn, if_exists="replace", index=False)

    silver_clean_df = pd.read_parquet(parquet_silver_clean_path)
    silver_clean_df.to_sql("silver_clean", conn, if_exists="replace", index=False)

    gold_df = pd.read_parquet(parquet_gold_path)
    gold_df.to_sql("gold", conn, if_exists="replace", index=False)

    conn.close()

    return {"registros": len(gold_df)}


# =======================================================
# MONITOR
# =======================================================
def to_iso(v):
    if v is None:
        return None
    return datetime.fromtimestamp(float(v)).isoformat()


def monitor_pipeline(**context):

    ti = context["ti"]
    task_ids = [
        "extract_bronze",
        "analisar_qualidade",
        "limpar_dados",
        "transformar_dados",
        "gerar_estatisticas",
        "load_database"
    ]

    registros = []
    for t in task_ids:

        row = {
            "task": t,
            "inicio": ti.xcom_pull(task_ids=t, key="start_time"),
            "fim": ti.xcom_pull(task_ids=t, key="end_time"),
            "duracao_seg": ti.xcom_pull(task_ids=t, key="duration"),
            "cpu_percent": ti.xcom_pull(task_ids=t, key="cpu_percent"),
            "memory_mb": ti.xcom_pull(task_ids=t, key="memory_mb"),
            "registros_processados": ti.xcom_pull(task_ids=t, key="registros"),
            "throughput_reg_s": ti.xcom_pull(task_ids=t, key="throughput"),
            "num_partitions": ti.xcom_pull(task_ids=t, key="num_partitions"),
            "num_cols": ti.xcom_pull(task_ids=t, key="num_cols"),
        }

        registros.append(row)

    df = pd.DataFrame(registros)

    # ============================================
    # 🔧 Diretório
    # ============================================
    monitor_dir = os.path.join(AIRFLOW_HOME, "data", "cnpj_abertos", "monitoramento")
    os.makedirs(monitor_dir, exist_ok=True)

    # ============================================
    # 📄 Salvar CSV
    # ============================================
    log_csv = os.path.join(monitor_dir, "monitoramento_log.csv")
    df.to_csv(log_csv, index=False)

    # ============================================
    # 📊 GRÁFICO 1 — Duração por Task
    # ============================================
    plt.figure(figsize=(12, 4))
    plt.bar(df["task"], df["duracao_seg"])
    plt.title("Duração por Task (segundos)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(monitor_dir, "duracao_por_task.png"))
    plt.close()

    # ============================================
    # 📊 GRÁFICO 2 — Uso de CPU por Task
    # ============================================
    plt.figure(figsize=(12, 4))
    plt.bar(df["task"], df["cpu_percent"])
    plt.title("Uso de CPU por Task (%)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(monitor_dir, "cpu_por_task.png"))
    plt.close()

    # ============================================
    # 📊 GRÁFICO 3 — Memória por Task
    # ============================================
    plt.figure(figsize=(12, 4))
    plt.bar(df["task"], df["memory_mb"])
    plt.title("Uso de Memória por Task (MB)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(monitor_dir, "memoria_por_task.png"))
    plt.close()

    return {"csv": log_csv}

# =======================================================
# DAG
# =======================================================
default_args = {
    'owner': 'professor',
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    'etl_cnpj_completo',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

task_1 = PythonOperator(task_id="extract_bronze", python_callable=extract_bronze, dag=dag)
task_2 = PythonOperator(task_id="analisar_qualidade", python_callable=analisar_qualidade, dag=dag)
task_3 = PythonOperator(task_id="limpar_dados", python_callable=limpar_dados, dag=dag)
task_4 = PythonOperator(task_id="transformar_dados", python_callable=transformar_dados, dag=dag)
task_5 = PythonOperator(task_id="gerar_estatisticas", python_callable=gerar_estatisticas, dag=dag)
task_6 = PythonOperator(task_id="load_database", python_callable=load_database, dag=dag)
task_7 = PythonOperator(task_id="monitorar", python_callable=monitor_pipeline, dag=dag)

task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7
