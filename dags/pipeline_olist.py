"""
Pipeline Olist: GitHub -> Snowflake
Data Integration e Pipelines — FIAP MBA Data Engineering
Prof. Rafael S Novo Pereira
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

GITHUB_BASE_URL = "https://raw.githubusercontent.com/rafsp/Aula2603_MBA/main/datasets"

OLIST_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
]

TEMP_DIR = "/tmp/olist_data"

default_args = {
    "owner": "fiap_data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _download_files():
    """Baixa CSVs do GitHub para /tmp local do pod."""
    import requests as req
    os.makedirs(TEMP_DIR, exist_ok=True)
    for f in OLIST_FILES:
        url = f"{GITHUB_BASE_URL}/{f}"
        print(f"Baixando {f}...")
        r = req.get(url, timeout=120)
        if r.status_code != 200:
            raise Exception(f"Erro {r.status_code} ao baixar {f}")
        with open(os.path.join(TEMP_DIR, f), "wb") as fh:
            fh.write(r.content)
        print(f"  OK: {f} ({len(r.content)/1024/1024:.1f} MB)")
    print(f"Todos os {len(OLIST_FILES)} arquivos baixados")


def _get_snowflake_conn():
    """Retorna conexao Snowflake a partir da Connection do Airflow."""
    from snowflake.connector import connect
    from airflow.hooks.base import BaseHook
    conn_info = BaseHook.get_connection("snowflake_default")
    extra = conn_info.extra_dejson if conn_info.extra_dejson else {}
    return connect(
        user=conn_info.login,
        password=conn_info.password,
        account=extra.get("account", ""),
        warehouse=extra.get("warehouse", "COMPUTE_WH"),
        database="OLIST_LAB",
        schema="PUBLIC",
        role=extra.get("role", "ACCOUNTADMIN"),
    )


def download_and_load(**kwargs):
    """Baixa CSVs do GitHub e carrega no Snowflake Bronze (mesmo pod)."""
    from snowflake.connector.pandas_tools import write_pandas
    import pandas as pd

    # Passo 1: Download
    _download_files()

    # Passo 2: Conectar no Snowflake e criar infra
    sf = _get_snowflake_conn()
    cur = sf.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS OLIST_LAB")
    cur.execute("USE DATABASE OLIST_LAB")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute("USE WAREHOUSE COMPUTE_WH")

    # Passo 3: Carregar cada CSV
    mapping = {
        "olist_orders_dataset.csv": "BRONZE_ORDERS",
        "olist_order_items_dataset.csv": "BRONZE_ORDER_ITEMS",
        "olist_order_payments_dataset.csv": "BRONZE_ORDER_PAYMENTS",
        "olist_order_reviews_dataset.csv": "BRONZE_ORDER_REVIEWS",
        "olist_customers_dataset.csv": "BRONZE_CUSTOMERS",
        "olist_sellers_dataset.csv": "BRONZE_SELLERS",
        "olist_products_dataset.csv": "BRONZE_PRODUCTS",
    }
    for fname, tbl in mapping.items():
        path = os.path.join(TEMP_DIR, fname)
        print(f"Carregando {fname} -> {tbl}...")
        df = pd.read_csv(path, low_memory=False)
        df.columns = [c.upper() for c in df.columns]
        cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        _, _, rows, _ = write_pandas(sf, df, tbl, auto_create_table=True)
        print(f"  OK: {rows:,} linhas em {tbl}")

    cur.close()
    sf.close()
    print("Bronze completa!")


def transform_silver(**kwargs):
    sf = _get_snowflake_conn()
    cur = sf.cursor()
    cur.execute("USE DATABASE OLIST_LAB")
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    cur.execute("""
    CREATE OR REPLACE TABLE SILVER_ORDERS AS
    SELECT
        o.ORDER_ID, o.CUSTOMER_ID, o.ORDER_STATUS,
        o.ORDER_PURCHASE_TIMESTAMP, o.ORDER_DELIVERED_CUSTOMER_DATE,
        o.ORDER_ESTIMATED_DELIVERY_DATE,
        c.CUSTOMER_CITY, c.CUSTOMER_STATE,
        i.TOTAL_ITENS, i.VALOR_PRODUTOS, i.VALOR_FRETE,
        p.TOTAL_PAGO, p.METODO_PRINCIPAL,
        DATEDIFF('day', TRY_TO_TIMESTAMP(o.ORDER_PURCHASE_TIMESTAMP),
            TRY_TO_TIMESTAMP(o.ORDER_DELIVERED_CUSTOMER_DATE)) AS DIAS_PARA_ENTREGA,
        CASE WHEN TRY_TO_TIMESTAMP(o.ORDER_DELIVERED_CUSTOMER_DATE) >
             TRY_TO_TIMESTAMP(o.ORDER_ESTIMATED_DELIVERY_DATE)
             THEN TRUE ELSE FALSE END AS ENTREGA_ATRASADA,
        TO_CHAR(TRY_TO_TIMESTAMP(o.ORDER_PURCHASE_TIMESTAMP), 'YYYY-MM') AS ANO_MES,
        CURRENT_TIMESTAMP() AS _PROCESSADO_EM
    FROM BRONZE_ORDERS o
    LEFT JOIN (
        SELECT ORDER_ID, COUNT(*) AS TOTAL_ITENS,
               ROUND(SUM(PRICE),2) AS VALOR_PRODUTOS,
               ROUND(SUM(FREIGHT_VALUE),2) AS VALOR_FRETE
        FROM BRONZE_ORDER_ITEMS GROUP BY ORDER_ID
    ) i ON o.ORDER_ID = i.ORDER_ID
    LEFT JOIN (
        SELECT ORDER_ID, ROUND(SUM(PAYMENT_VALUE),2) AS TOTAL_PAGO,
               MIN(PAYMENT_TYPE) AS METODO_PRINCIPAL
        FROM BRONZE_ORDER_PAYMENTS GROUP BY ORDER_ID
    ) p ON o.ORDER_ID = p.ORDER_ID
    LEFT JOIN BRONZE_CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
    """)
    cur.execute("SELECT COUNT(*) FROM SILVER_ORDERS")
    cnt = cur.fetchone()[0]
    print(f"Silver criada: {cnt:,} pedidos")
    cur.close()
    sf.close()


def build_gold(**kwargs):
    sf = _get_snowflake_conn()
    cur = sf.cursor()
    cur.execute("USE DATABASE OLIST_LAB")
    cur.execute("USE WAREHOUSE COMPUTE_WH")

    cur.execute("""
    CREATE OR REPLACE TABLE GOLD_RECEITA_ESTADO AS
    SELECT CUSTOMER_STATE AS ESTADO, COUNT(*) AS TOTAL_PEDIDOS,
           ROUND(SUM(TOTAL_PAGO),2) AS RECEITA_TOTAL,
           ROUND(AVG(TOTAL_PAGO),2) AS TICKET_MEDIO,
           ROUND(AVG(DIAS_PARA_ENTREGA),1) AS DIAS_ENTREGA_MEDIO,
           ROUND(SUM(CASE WHEN ENTREGA_ATRASADA THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS PCT_ATRASO,
           CURRENT_TIMESTAMP() AS _ATUALIZADO_EM
    FROM SILVER_ORDERS WHERE CUSTOMER_STATE IS NOT NULL
    GROUP BY CUSTOMER_STATE ORDER BY RECEITA_TOTAL DESC
    """)

    cur.execute("""
    CREATE OR REPLACE TABLE GOLD_ANALISE_REVIEWS AS
    SELECT r.REVIEW_SCORE AS NOTA, COUNT(*) AS TOTAL_REVIEWS,
           ROUND(AVG(so.DIAS_PARA_ENTREGA),1) AS DIAS_ENTREGA_MEDIO,
           ROUND(SUM(CASE WHEN so.ENTREGA_ATRASADA THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS PCT_ATRASADO,
           CURRENT_TIMESTAMP() AS _ATUALIZADO_EM
    FROM BRONZE_ORDER_REVIEWS r
    LEFT JOIN SILVER_ORDERS so ON r.ORDER_ID = so.ORDER_ID
    GROUP BY r.REVIEW_SCORE ORDER BY NOTA
    """)

    cur.execute("SELECT COUNT(*) FROM GOLD_RECEITA_ESTADO")
    print(f"Gold Receita: {cur.fetchone()[0]} estados")
    cur.execute("SELECT COUNT(*) FROM GOLD_ANALISE_REVIEWS")
    print(f"Gold Reviews: {cur.fetchone()[0]} notas")
    cur.close()
    sf.close()


def quality_checks(**kwargs):
    sf = _get_snowflake_conn()
    cur = sf.cursor()
    checks = []

    for tbl, minr in [("BRONZE_ORDERS",90000),("BRONZE_ORDER_ITEMS",100000),
                       ("BRONZE_CUSTOMERS",90000),("BRONZE_SELLERS",3000)]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        c = cur.fetchone()[0]
        checks.append((f"Bronze: {tbl}", "PASS" if c >= minr else "FAIL", f"{c:,} (min {minr:,})"))

    cur.execute("SELECT COUNT(*) FROM SILVER_ORDERS")
    sc = cur.fetchone()[0]
    checks.append(("Silver: SILVER_ORDERS", "PASS" if sc > 90000 else "FAIL", f"{sc:,}"))

    for gt in ["GOLD_RECEITA_ESTADO","GOLD_ANALISE_REVIEWS"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {gt}")
            c = cur.fetchone()[0]
            checks.append((f"Gold: {gt}", "PASS" if c > 0 else "FAIL", f"{c}"))
        except:
            checks.append((f"Gold: {gt}", "FAIL", "nao existe"))

    cur.close()
    sf.close()

    print("=" * 60)
    print("  RELATORIO DE QUALIDADE")
    print("=" * 60)
    for n, s, d in checks:
        print(f"  [{s}] {n}: {d}")
    print("=" * 60)
    fails = [c for c in checks if c[1] == "FAIL"]
    if fails:
        raise ValueError(f"{len(fails)} checks falharam!")
    print("Todos os checks passaram!")


with DAG(
    dag_id="pipeline_olist_completo",
    default_args=default_args,
    description="Pipeline: GitHub -> Snowflake Bronze/Silver/Gold",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "fiap", "snowflake"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    t1 = PythonOperator(task_id="download_and_load_bronze", python_callable=download_and_load)
    t2 = PythonOperator(task_id="transform_silver", python_callable=transform_silver)
    t3 = PythonOperator(task_id="build_gold", python_callable=build_gold)
    t4 = PythonOperator(task_id="quality_checks", python_callable=quality_checks)

    fim = EmptyOperator(task_id="fim")

    inicio >> t1 >> t2 >> t3 >> t4 >> fim