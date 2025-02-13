#importing necessary libraries
import pandas as pd
import requests
from io import StringIO
import time
import json
import yfinance as yf
from datetime import datetime
from bs4 import BeautifulSoup

#importing airflow for an orchestration
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook



def _scrap_yahoo_finance_data(**kwargs):
    # Since there is no API on the market that allows fetch top companies by market cap. We will pull the data using bs4 package from html page of yahoo top listed companies
    url = "https://finance.yahoo.com/research-hub/screener/LARGEST_MARKET_CAP/?start-0&count=100"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    symbols = [span.text.strip() for span in soup.find_all("span", class_="symbol")]
    kwargs["ti"].xcom_push(key="symbols", value=symbols)

def upload_to_azure(data, container_name, blob_name):
    """Uploads a CSV string to Azure Blob Storage."""
    wasb_hook = WasbHook(wasb_conn_id='azure_blob_storage_conn_id')
    wasb_hook.load_string(
        string_data=data,
        container_name=container_name,
        blob_name=blob_name,
        overwrite=True  
    )

def _process_profit_and_loss(**kwargs):
    symbols = kwargs["ti"].xcom_pull(task_ids="scrap_yahoo_finance_data", key="symbols")
    profit_and_loss = []
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        data = pd.DataFrame(stock.quarterly_financials)
        data['symbol'] = symbol
        data['type'] = 'P&L'
        profit_and_loss.append(data)
        time.sleep(1)

    df_profit_and_loss = pd.concat(profit_and_loss)
    df_profit_and_loss = df_profit_and_loss.reset_index().rename(columns={'index': 'Description'})
    

    df_profit_and_loss_buffer = StringIO()
    df_profit_and_loss.to_csv(df_profit_and_loss_buffer, index=False)
    profit_and_loss_csv = df_profit_and_loss_buffer.getvalue()

 
    upload_to_azure(profit_and_loss_csv, 'finance-etl', f'raw_data/profit_and_loss/pl_transformed_{kwargs["ds"]}.csv')

def _process_balance_sheet(**kwargs):
    symbols = kwargs["ti"].xcom_pull(task_ids="scrap_yahoo_finance_data", key="symbols")
    balance_sheet = []
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        data = pd.DataFrame(stock.quarterly_balance_sheet)
        data['symbol'] = symbol
        data['type'] = 'BS'
        balance_sheet.append(data)
        time.sleep(1)

    df_balance_sheet = pd.concat(balance_sheet)
    df_balance_sheet = df_balance_sheet.reset_index().rename(columns={'index': 'Description'})

 
    df_balance_sheet_buffer = StringIO()
    df_balance_sheet.to_csv(df_balance_sheet_buffer, index=False)
    balance_sheet_csv = df_balance_sheet_buffer.getvalue()

 
    upload_to_azure(balance_sheet_csv, 'finance-etl', f'raw_data/balance_sheet/bs_transformed_{kwargs["ds"]}.csv')

def _process_cashflow(**kwargs):
    symbols = kwargs["ti"].xcom_pull(task_ids="scrap_yahoo_finance_data", key="symbols")
    cash_flow = []
    for symbol in symbols:
        stock = yf.Ticker(symbol)
        data = pd.DataFrame(stock.quarterly_cashflow)
        data['symbol'] = symbol
        data['type'] = 'CF'
        cash_flow.append(data)
        time.sleep(1)

    df_cashflow = pd.concat(cash_flow)
    df_cashflow = df_cashflow.reset_index().rename(columns={'index': 'Description'})

 
    df_cashflow_buffer = StringIO()
    df_cashflow.to_csv(df_cashflow_buffer, index=False)
    cashflow_csv = df_cashflow_buffer.getvalue()

   
    upload_to_azure(cashflow_csv, 'finance-etl', f'raw_data/cashflow/cf_transformed_{kwargs["ds"]}.csv')



    
##### DAG

default_args = {
    "owner":"airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 26),
    }


dag = DAG(
    dag_id="fetch_and_upload_to_azure",
    default_args=default_args,
    description="Upload financial data to Azure Blob Storage",
    schedule_interval=None,
    catchup=False
)

##### Operators
operator_scrap_yahoo_data = PythonOperator(
    task_id='scrap_yahoo_finance_data',
    python_callable=_scrap_yahoo_finance_data,
    provide_context=True,
    dag=dag
)

operator_process_profit_and_loss_data = PythonOperator(
    task_id='process_pl_data',
    python_callable=_process_profit_and_loss,
    provide_context=True,
    dag=dag
)

operator_process_balance_sheet_data = PythonOperator(
    task_id='process_bs_data',
    python_callable=_process_balance_sheet,
    provide_context=True,
    dag=dag
)

operator_process_cashflow_data = PythonOperator(
    task_id='process_cf_data',
    python_callable=_process_cashflow,
    provide_context=True,
    dag=dag
)

## Flow

operator_scrap_yahoo_data >> operator_process_profit_and_loss_data
operator_scrap_yahoo_data >> operator_process_balance_sheet_data
operator_scrap_yahoo_data >> operator_process_cashflow_data



    























   