import os
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
sys.path.append('/datafuel/')
import unicodedata
import re


default_args = {
    'owner': 'YKA',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['datalake'])
def load_entrees_sorties_data(
    data_path: str = "/Data_cm",
    sorties_csv: str = "sorties.csv",
    entree_csv: str = "entree.csv",
    dwh_schema: str = "stg",
    entree_table: str = "entree",
    sorties_table: str = "sorties",
    postgres_url: str = "postgres://postgres:password4db@postgres_dwh:5432/dwhdb"
):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    def process_columns(string_to_process, replace_string='_'):
        """
        Process a column name by :
        - processing special aplphabetical characters
        - replacing special characters by value set in 'replace_string' 

        #Parameters
        :param string_to_process (str): String to process (column name)
        :param replace_string (str, default : '_'): Value to set for replaced characters
            (ex : "/" -> "", where "" is the replace_string)

        #Return
        return out (str): Processed string
        """
        BIGQUERY_COLUMN_NAME_RE = "[^0-9a-zA-Z_]+"
        # Get encoding of column
        str_bytes = str.encode(string_to_process)
        encoding = 'utf-8'

        # Process any special alphabetical character
        temp_out = unicodedata\
            .normalize('NFKD', string_to_process)\
            .encode('ASCII', 'ignore')\
            .decode(encoding)

        out = re.sub(BIGQUERY_COLUMN_NAME_RE, replace_string, temp_out)

        return out
        
    @task()
    def transform_csv_to_df_to_table(
        data_path: str, 
        csv_name: str, 
        postgres_url: str,
        dwh_schema: str,
        table_name: str
    ) :
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        csv_path = f"{data_path}/{csv_name}"

        df = pd.read_csv(csv_path)

        ls_col = []
        for column in df.columns :
            column_clean = process_columns(column)
            ls_col.append(column_clean)
        df.columns = ls_col

        # creer la connexion a la base de donées
        engine = create_engine(postgres_url)

        # creer le schema du DWH, engine.execute c'est tjr du SQL
        engine.execute(f"CREATE SCHEMA IF NOT EXISTS {dwh_schema}") 

        # charger le df dans une table de la base de données
        df.to_sql(name=table_name,con=engine,schema=dwh_schema,if_exists="replace")
            
            
           
    transform_csv_to_df_to_table(
        data_path = data_path, 
        csv_name = sorties_csv, 
        postgres_url = postgres_url,
        dwh_schema = dwh_schema,
        table_name = sorties_table
    )
        
    transform_csv_to_df_to_table(
        data_path = data_path, 
        csv_name = entree_csv, 
        postgres_url = postgres_url,
        dwh_schema = dwh_schema,
        table_name = entree_table
    )
load_entrees_sorties_data_dag = load_entrees_sorties_data()
