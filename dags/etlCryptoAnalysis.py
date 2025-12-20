from airflow import DAG
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime, timedelta
from pathlib import Path
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.decorators import task
from airflow.models import Variable
from zoneinfo import ZoneInfo




BINANCE_URL = "https://api.binance.com/api/v3/klines"
chat_id = Variable.get("TELEGRAM_CHAT_ID")



def fetch_and_store_ohlcv(symbol):
        # symbol = "BTCUSDT"
        interval = "1h"
        limit = 5

        # 1. Call API
        res = requests.get(
            BINANCE_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            },
            timeout=30
        )
        res.raise_for_status()
        data = res.json()

        # 2. Connect Postgres
        hook = PostgresHook(postgres_conn_id="airflow_db")
        conn = hook.get_conn()
        cur = conn.cursor()



        # 4. Insert data
        insert_sql = """
            INSERT INTO dev.rd_ohlcv (
                symbol, interval,
                open_time, close_time,
                open, high, low, close,
                volume, quote_volume,
                trades_count,
                taker_buy_base_volume,
                taker_buy_quote_volume
            )
            VALUES (
                %s, %s,
                to_timestamp(%s / 1000.0),
                to_timestamp(%s / 1000.0),
                %s, %s, %s, %s,
                %s, %s,
                %s,
                %s, %s
            )
        """

        for c in data:
            cur.execute(
                insert_sql,
                (
                    symbol, interval,
                    c[0], c[6],      # open_time, close_time
                    c[1], c[2], c[3], c[4],
                    c[5], c[7],
                    c[8],
                    c[9], c[10]
                )
            )

        conn.commit()
        cur.close()
        conn.close()

def to_float(x):
    return round(float(x), 2) if x is not None else 'N/A'

def get_message_text():
    hook = PostgresHook(postgres_conn_id="airflow_db")
    conn = hook.get_conn()
    cur = conn.cursor()

    sql = """
        with tmp as (
        select symbol, open_time, current_close ,prev_close ,pct_change,
        ROW_NUMBER() over (PARTITION BY symbol order by open_time desc) r
        from dev.ohlcv_price_change
        )
        select * from tmp 
        where r = 1"""
    cur.execute(sql)
    rows = cur.fetchall()
    message = "📊 <b>Latest OHLCV Price Change</b>\n\n"
    for r in rows:
        symbol = r[0]
        open_time = r[1]
        dt_sg = open_time.astimezone(ZoneInfo("Asia/Singapore"))

        open_time_str = dt_sg.strftime("%Y-%m-%d %H:%M")



        current_close = to_float(r[2])
        prev_close = to_float(r[3])
        pct_change = to_float(r[4])

        # message like table
        message += (
            f"🔹 <b>{symbol}</b>\n"
            f"   ⏰ {open_time_str} (SGT)\n"
            f"   💰 Current Close: {current_close}\n"
            f"   💵 Previous Close: {prev_close}\n"
            f"   📈 % Change: {pct_change}%\n\n"
        )


    cur.close()
    conn.close()
    return message


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow_db",
        profile_args={"schema": "dev"},
    )
)


default_args = {
    'owner': 'airflow',
    'retries': 1,
}


with DAG(
    dag_id='crypto_analysis_pipeline',
    default_args=default_args,
    schedule='5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    

    @task
    def get_symbols_fech_store():
        hook = PostgresHook(postgres_conn_id="airflow_db")
        conn = hook.get_conn()
        cur = conn.cursor()

        sql = """
            SELECT DISTINCT symbol
            FROM dev.crypto_assets
            where is_active = true
        """

        cur.execute(sql)
        rows = cur.fetchall()


        delete_sql = """
            DELETE FROM dev.rd_ohlcv
        """
        print("Clearing existing OHLCV data...")
        cur.execute(delete_sql)
        conn.commit()



        cur.close()
        conn.close()

        symbols = [r[0] for r in rows]
        for s in symbols:
            print(f"Fetched symbol: {s}")
            fetch_and_store_ohlcv(s)    
    



    dbt_group = DbtTaskGroup(
        group_id="crypto_analysis",
        project_config=ProjectConfig(
            str(Path(__file__).parent / "dbtproject" / "cryptoAnalysis")
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path = "/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        profile_config=profile_config,
    )


    @task
    def send_test():
        hook = TelegramHook(telegram_conn_id="telegram_id")

        message = get_message_text()


        hook.send_message(api_params={
            "text": message,
            "parse_mode": "HTML",
            "chat_id": chat_id
        }
    )

    ohlcv_task = get_symbols_fech_store()
    send_test_task = send_test()
    ohlcv_task >> dbt_group >> send_test_task



