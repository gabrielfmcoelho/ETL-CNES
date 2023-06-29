import os
import pandas as pd
from google.cloud import bigquery
import numpy as np

class ConnectionBigQuery:
    def __init__(self, credentials):
        self.credentials = credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials
        self.client = bigquery.Client()

    def verify_max_date(self, table_id):
        try:
            table = self.client.get_table(table_id)
            return int(table.description)
        except:
            return 0

    def InsertIntoTable(self, df, table_id, date):
        table = bigquery.Table(table_id)
        try:
            table = self.client.create_table(table)  # Cria a tabela
        except:
            table = self.client.get_table(table_id)  # Se a tabela já existir, pega a tabela
        try:
            if int(table.description) >= date:
                print(f"Não há dados novos para {table_id}")
                return
        except:
            pass
        try:
            df_splits = np.array_split(df, 10) # Divide o dataframe em 10 partes
            i=0
            for df_split in df_splits:
                rows_to_insert = df_split.to_dict('records')
                print(f"Carregando {len(rows_to_insert)} linhas para {table_id} (parte {i+1} de {len(df_splits)})")
                response = self.client.insert_rows(table, rows_to_insert)
                i+=1
            table.description = str(date)
            self.client.update_table(table, ['description']) # Atualiza a descrição da tabela
            print(f"Carregamento concluído para {table_id}")

        except Exception as ex:
            print(f"Ocorreu o seguinte erro: {ex} {response}")

    def InsertDataFrame(self, df, table_id, date):
        table = bigquery.Table(table_id)
        try:
            table = self.client.create_table(table)  # Cria a tabela
        except:
            table = self.client.get_table(table_id)  # Se a tabela já existir, pega a tabela
        try:
            job = self.client.load_table_from_dataframe(df, table_id)
            print(job.result())
            table.description = str(date)
            self.client.update_table(table, ['description']) # Atualiza a descrição da tabela
            print(f"Carregamento concluído para {table_id}")
        except Exception as ex:
            print(f"Ocorreu o seguinte erro: {ex}")

        
        
        
