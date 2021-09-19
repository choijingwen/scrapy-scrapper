import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import settings
from airflow.models import Connection


try:
    conn = Connection(
            conn_id='dev_db',
            conn_type='postgres',
            host='postgres',
            login='airflow',
            password='airflow',
            port=5432,
            schema='airflow'
    )
    session = settings.Session()
    session.add(conn)
    session.commit()
except:
    pass


class TableScrapper():

    def __init__(self):
        self.base_url = 'https://www.malaysiastock.biz/Listed-Companies.aspx?type=A&value='
        self.first_page_url = self.base_url+'0'
        self.all_pages = self.retrive_available_pages()
        self.data = {
            'COMPANY_NAME': [],
            'STOCK_NAME': [],
            'STOCK_CODE': [],
            'MARKET_NAME': [],
            'SYARIAH': [],
            'SECTOR': [],
            'MARKET_CAP': [],
            'LAST_PRICE': [],
            'PE_RATIO': [],
            'DY': [],
            'ROE': []
        }
        self.data_mapper = {
            '2': 'SECTOR',
            '3': 'MARKET_CAP',
            '4': 'LAST_PRICE',
            '5': 'PE_RATIO',
            '6': 'DY',
            '7': 'ROE'
        }

    def retrive_available_pages(self):
        response = requests.get(self.first_page_url)
        soup = BeautifulSoup(response.text, "lxml")

        ## Retrieve the filtering selection pages
        available_pages = []
        filter_selection = soup.findAll('td', {"class": "filteringSelection"})
        for page in filter_selection:
            available_pages.append(page.a.get('href').split("=")[-1])
        
        return available_pages

    def retrive_table_content(self, on_page):
        url = self.base_url+on_page
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "lxml")

        ## Retrive the table content
        table = soup.findAll('table', {"class": "marketWatch"})[0]

        for row in table.findAll(['tr'])[1:]:
            cols = row.find_all('td')
            for index, col in enumerate(cols):
                if index == 0:
                    a_groups = col.a.text.strip().split(" ")
                    self.data['STOCK_NAME'].append(a_groups[0])
                    self.data['STOCK_CODE'].append(a_groups[1][1:-1])
                    try:
                        self.data['MARKET_NAME'].append(col.span.text)
                    except:
                        self.data['MARKET_NAME'].append(None)
                    self.data['COMPANY_NAME'].append(col.find_all('h3')[-1].text.strip())
                elif index == 1:
                    if col.img['src'].endswith('Yes.png'):
                        self.data['SYARIAH'].append(True)
                    elif col.img['src'].endswith('No.png'):
                        self.data['SYARIAH'].append(False)
                    else:
                        self.data['SYARIAH'].append(None)
                else:
                    self.data[self.data_mapper[str(index)]].append(col.text.strip())


def get_data_from_website():
    ## Scrap data from website table by pages
    ts = TableScrapper()
    ts_pages = ts.all_pages

    for ts_page in ts_pages:
        ts.retrive_table_content(on_page=ts_page)
    
    df_data = ts.data

    ## Write the scrapped data into a csv format
    df = pd.DataFrame(df_data)
    df['UPDATED_TS'] =  dt.datetime.now()
    df.to_csv('/opt/airflow/data/raw_table_output.csv', index=False)


def load_raw_data():
    conn = PostgresHook(postgres_conn_id='dev_db').get_conn()
    cur = conn.cursor()
    SQL_STATEMENT = """
        COPY raw_table FROM STDIN DELIMITER ',' CSV HEADER
        """
    with open('/opt/airflow/data/raw_table_output.csv', 'r') as f:
        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()


# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'web_scrapper',
    description='Web Scrapper DAG',
    schedule_interval='0 12 * * *',
    start_date=datetime(2017, 3, 20),
    catchup=False,
    is_paused_upon_creation=False
)

# Task 0: Echo task start
task_start = BashOperator(
    task_id = 'start_task',
    bash_command = 'echo start',
    dag = dag
)

# Task 1: Scraping data from website
web_scraping = PythonOperator(
    task_id = 'get_data_from_website',
    python_callable = get_data_from_website,
    dag = dag
)

# Task 2: Create raw data table
create_raw_table = PostgresOperator(
    task_id="create_raw_table",
    postgres_conn_id="dev_db",
    sql="sql/raw_schema.sql",
)

# Task 3: Load raw scrapped data to the table
raw_data_loading = PythonOperator(
    task_id = 'raw_data_loading',
    python_callable = load_raw_data,
    dag = dag
)

# Task 4: Create fact data table
create_fact_table = PostgresOperator(
    task_id="create_fact_table",
    postgres_conn_id="dev_db",
    sql="sql/fact_schema.sql",
)

# Task 5: Create dimension data table
create_dimension_table = PostgresOperator(
    task_id="create_dimension_table",
    postgres_conn_id="dev_db",
    sql="sql/dimension_schema.sql",
)

# Set up the dependencies
task_start >> web_scraping >> create_raw_table >> raw_data_loading
raw_data_loading.set_downstream(create_fact_table)
raw_data_loading.set_downstream(create_dimension_table)
