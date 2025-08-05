from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pytz
from collections import defaultdict

def aggregate_stock_ticks():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    ny_tz = pytz.timezone("America/New_York")
    today_ny = datetime.now(ny_tz).date()

    cursor.execute("""
        SELECT symbol, price, timestamp
        FROM stock_ticks_stream
    """)
    rows = cursor.fetchall()

    filtered = []
    for symbol, price, ts in rows:
        ts_local = ts.astimezone(ny_tz)
        if ts_local.date() == today_ny:
            filtered.append((symbol, price, ts_local))

    grouped = defaultdict(list)
    for symbol, price, ts_local in filtered:
        grouped[symbol].append((ts_local, price))

    summary_rows = []
    for symbol, records in grouped.items():
        records.sort()  
        open_price = records[0][1]
        close_price = records[-1][1]
        prices = [p for _, p in records]
        high = max(prices)
        low = min(prices)

        summary_rows.append((symbol, today_ny, open_price, high, low, close_price))

    insert_sql = """
        INSERT INTO stock_eod_summary (symbol, date, open, high, low, close)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close;
    """
    cursor.executemany(insert_sql, summary_rows)
    conn.commit()
    cursor.close()


default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='stock_eod_summary_dag',
    default_args=default_args,
    schedule_interval='0 21 * * 1-5',
    catchup=False,
    max_active_runs=1
) as dag:

    aggregate_task = PythonOperator(
        task_id='aggregate_stock_ticks',
        python_callable=aggregate_stock_ticks
    )