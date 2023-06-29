import pandas as pd
import numpy as np
import warnings
import sys
import os
import re
warnings.filterwarnings("ignore", category=Warning)

TARGET_TABLES = [
    'DemografiaPI',
    'labs',
]

def recive_args():
    if len(sys.argv) < 2:
        print('Missing arguments')
        print('Usage: python labs.py <path_datasets>')
        sys.exit(1)
    elif len(sys.argv) > 1:
        return sys.argv[1], TARGET_TABLES
    else:
        return './Datasets', TARGET_TABLES

def compile_info(path_tables):
    lista_colunas_demografia = ['CO_MUNICIPIO_GESTOR',
                                'Municipio',
                                'Mesoregiao',
                                'RegionaldeSaude',
                                ]                      
    df = pd.read_csv(path_tables[0], sep=',', usecols=lista_colunas_demografia)
    df['CO_MUNICIPIO_GESTOR'] = df['CO_MUNICIPIO_GESTOR'].astype(str)

    df_aux = pd.read_csv(path_tables[1], sep=';')
    df_aux['CO_MUNICIPIO_GESTOR'] = df_aux['CO_MUNICIPIO_GESTOR'].astype(str)
    
    df_final = df.merge(df_aux, how='left', on='CO_MUNICIPIO_GESTOR')
    print(df_final.head(14))
    df_final.drop(columns=['DT_DADOS'], inplace=True)
    df_final.drop_duplicates(inplace=True)
    print(df_final.head(14))
    
    df_final = pd.get_dummies(df_final, columns = ['CO_CONVENIO'])
    print(df_final.head(14))
    df_final = df_final.groupby(['CO_UNIDADE']).agg({'CO_MUNICIPIO_GESTOR':'first', 'Municipio':'first', 'Mesoregiao':'first', 'RegionaldeSaude':'first', 'NU_CNPJ_MANTENEDORA':'first', 'NO_FANTASIA':'first', 'CO_CEP':'first', 'NU_TELEFONE':'first', 'NO_EMAIL':'first', 'CO_ATIVIDADE':'nunique', 'CO_TIPO_ESTABELECIMENTO':'nunique', 'CO_CONVENIO_1.0':'sum', 'CO_CONVENIO_2.0':'sum', 'CO_CONVENIO_5.0':'sum', 'CO_CONVENIO_6.0':'sum'}).reset_index()
    #Rename CONVENIO_1.0 TO CONVENIO_SUS
    df_final.rename(columns={'CO_CONVENIO_1.0':'CO_CONVENIO_SUS', 'CO_CONVENIO_2.0':'CO_CONVENIO_PARTICULAR', 'CO_CONVENIO_5.0':'CO_CONVENIO_PLPUB', 'CO_CONVENIO_6.0':'CO_CONVENIO_PSPRIV'}, inplace=True)
    return df_final

def save_on_final(df, path_datasets, filename):
    df.to_csv(path_datasets+'/'+ filename[-21:-4] + 'cp' + '.csv', index=False)

if __name__ == "__main__":
    print('Running merging of labs.py...')
    path_datasets, target_tables = recive_args()
    path_tables = []
    for target in target_tables:
        for file in os.listdir(path_datasets):
            if re.search(target,file):
                if re.search('labs',target) and re.search('cp',target):
                    continue
                path_tables.append(path_datasets + '/' + file)
    print(path_tables)
    df = compile_info(path_tables)
    save_on_final(df, path_datasets, path_tables[1])
    print('Script merging of labs concluded')