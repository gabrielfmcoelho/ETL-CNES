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
    'pacientes_env',
    'pacientes_rec',
]

def recive_args():
    if len(sys.argv) < 4:
        print('Missing arguments')
        print('Usage: python labs.py <path_datasets>')
        sys.exit(1)
    elif len(sys.argv) > 3:
        return sys.argv[1], TARGET_TABLES
    else:
        return './Datasets', TARGET_TABLES

def compile_info(path_tables):
    lista_colunas_demografia = ['CO_MUNICIPIO_GESTOR',
                                'Municipio',
                                'Mesoregiao',
                                'RegionaldeSaude',
                                'DensidadeDemografica_2010_',
                                'PopulacaoEstimada_2021_',
                                'IdadeMediaPop',
                                'PopRelPediatrica',
                                'PopRelIdoso',
                                'IDHM_2010_',
                                'PIBPerCapita_2020_',]                      
    df = pd.read_csv(path_tables[0], sep=',', usecols=lista_colunas_demografia)
    df['CO_MUNICIPIO_GESTOR'] = df['CO_MUNICIPIO_GESTOR'].astype(str)
    df = df.groupby(['CO_MUNICIPIO_GESTOR']).agg({'QTLeitosSUS':'median', 'QT_NaoSUS':'median', 'QTLeitosExist':'median', 'DVLeitos':'max', 'EXTLeitosPediatrico':'max', "QTProfissionais":"median", "QTUnidades":"max", "QTCNES":"max", "DVtpunidade":"max", "DVcotpunidade":"max", "DVcotpestab":"max","QTHospitais":"max", "QTClinicas":"max", "QTLaboratorios":"max", "QTConsultorios":"max", "QTUnidadesBasicas":"max"}).reset_index()
    df_aux['DensidadeDemografica_2010_'] = df_aux['DensidadeDemografica_2010_'].str.replace(',', '.').astype(float)
    df_aux['IdadeMediaPop'] = df_aux['IdadeMediaPop'].str.replace(',', '.').astype(float)
    df_aux['PopRelPediatrica'] = df_aux['PopRelPediatrica'].str.replace(',', '.').astype(float)
    df_aux['PopRelIdoso'] = df_aux['PopRelIdoso'].str.replace(',', '.').astype(float)
    df_aux['PIBPerCapita_2020_'] = df_aux['PIBPerCapita_2020_'].str.replace(',', '.').astype(float)

    df_aux = pd.read_csv(path_tables[1])
    df_aux2 = pd.read_csv(path_tables[2])
    df_aux3 = pd.read_csv(path_tables[3])
    
    df_final = df.merge(df_aux, how='left', on='CO_MUNICIPIO_GESTOR')
    df_final = df_final.merge(df_aux2, how='left', on='CO_MUNICIPIO_GESTOR')
    df_final = df_final.merge(df_aux3, how='left', on='CO_MUNICIPIO_GESTOR')
    df_final['PacientesEnv'].fillna(0, inplace=True)
    df_final['PacientesRec'].fillna(0, inplace=True)
    return df_final

def save_on_final(df, path_datasets, filename):
    df.to_csv(path_datasets+'/'+ filename[-17:-4] + 'cp' + '.csv', index=False)

if __name__ == "__main__":
    print('Running merging of labs.py...')
    path_datasets, target_tables = recive_args()
    path_tables = []
    for target in target_tables:
        if target in os.listdir(path_datasets):
            if re.search('labs',target) and re.search('cp',target):
                continue
            path_tables += [path_datasets + '/' + target]
    df = compile_info(path_tables)
    save_on_final(df, path_datasets, path_tables[1])
    print('Script merging of labs concluded')