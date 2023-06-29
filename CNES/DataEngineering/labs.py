import pandas as pd
import numpy as np
import sys
import re
import os
import warnings
warnings.filterwarnings("ignore", category=Warning)

TARGET_TABLES = ['tbEstabelecimento',
                   'tbTipoEstabelecimento',
                   'tbMunicipio',
                   'rlAtividadeObrigatoria',
                   'rlEstabAtendPrestConv',
                   'rlEstabComplementar',
                   'tbLeito',
                   'rlEstabRegimeRes',
                   'tbAtendimentoPrestado',
                   'tbAtividade',
                   'tbNaturezaJuridica',
                   'tbConvenio',
                   'rlEstabEquipeProf',
                   'tbAtividadeProfissional',
                   'rlAdmGerenciaCnes',
                   'rlEstabRepresentante',
                   'rlEstabEndCompl']

def recive_args():
    if len(sys.argv) < 4:
        print('Missing arguments')
        print('Usage: python labs.py <path_unziped> <path_final> <target_tables> <date>')
        sys.exit(1)
    elif len(sys.argv) > 3:
        return sys.argv[1], sys.argv[2], TARGET_TABLES, sys.argv[3]
    else:
        return './CNES/Uziped', './CNES/Final', TARGET_TABLES, '202201'

def compile_info(path_tables, date):
    #-------------------------CONTABILIZAÇÃO DE LEITOS-------------------------
    # UNIÃO DE TBESTABELECIMENTO -> RLESTABCOMPLEMENTAR -> RLESTABATENDPRESTCONV = DF_FINAL_1
    ## Tabela de informações sobre os estabelecimentos -> TBESTABELECIMENTO
    table = path_tables[0]
    lista_colunas = [
        'CO_UNIDADE',
        'CO_CNES',
        'CO_ATIVIDADE',
        'TP_UNIDADE',
        'CO_ESTADO_GESTOR',
        'CO_MUNICIPIO_GESTOR',
        'CO_TIPO_UNIDADE',
        'CO_TIPO_ESTABELECIMENTO',
        'NU_CNPJ_MANTENEDORA',
        'TP_PFPJ',
        'NO_FANTASIA',
        'NU_ENDERECO',
        'CO_CEP',
        'NU_TELEFONE',
        'NO_EMAIL'
    ]
    df_main = pd.read_csv(table, sep=';', encoding='utf-8', usecols=lista_colunas)
    ## DATAWRANGLING - Trocando tipo de colunas
    df_main['CO_CNES'] = df_main['CO_CNES'].astype(str)
    df_main['CO_MUNICIPIO_GESTOR'] = df_main['CO_MUNICIPIO_GESTOR'].astype(str)
    df_main['CO_ESTADO_GESTOR'] = df_main['CO_ESTADO_GESTOR'].astype(str)
    df_main['CO_UNIDADE'] = df_main['CO_UNIDADE'].astype(str)
    df_main['TP_UNIDADE'] = df_main['TP_UNIDADE'].astype(str)
    df_main['CO_TIPO_UNIDADE'] = df_main['CO_TIPO_UNIDADE'].astype(str)
    df_main['CO_TIPO_ESTABELECIMENTO'] = df_main['CO_TIPO_ESTABELECIMENTO'].astype(str)
    ## LIMPEZA DE DADOS - Remoção de registros com CO_ESTADO_GESTOR diferente de 22 (PI)
    df_main = df_main.loc[df_main['CO_ESTADO_GESTOR'] == '22']
    ## LIMPEZA DE DADOS - Remoção de registros com TP_UNIDADE diferente de 5 e 7
    df_final = df_main.drop(df_main[(df_main['TP_UNIDADE'] != '39') & (df_main['TP_UNIDADE'] != '67') & (df_main['TP_UNIDADE'] != '80')].index).copy()

    ## RLESTABATENDPRESTCONV - tabela de dados de atendimento de leitos e convênios
    table = path_tables[4]
    lista_colunas = [
        'CO_UNIDADE',
        'CO_CONVENIO',
    ]
    df_aux = pd.read_csv(table, sep=';', encoding='utf-8', usecols=lista_colunas)
    ## DATAWRANGLING - Trocando tipo de colunas
    df_aux['CO_UNIDADE'] = df_aux['CO_UNIDADE'].astype(str)
    df_aux['CO_CONVENIO'] = df_aux['CO_CONVENIO'].astype(str)
    ## LIMPEZA DE DADOS - Remoção de registros com CO_CONVENIO diferente de 3, 4, 5 e 6
    #df_aux = df_aux.drop(df_aux[(df_aux['CO_CONVENIO'] != '3') & (df_aux['CO_CONVENIO'] != '4') & (df_aux['CO_CONVENIO'] != '5') & (df_aux['CO_CONVENIO'] != '6')].index)
    ## MERGE - DF_FINAL <- TBATENDIMENTOPRESTADO (left)
    df_final = df_final.merge(df_aux, how='left', on='CO_UNIDADE')

    #-------------------------CONTABILIZAÇÃO DE PROFISSIONAIS DA SAUDE---------
    table = path_tables[12]
    df_final_2 = pd.read_csv(table, sep=';', encoding='utf-8', usecols=['CO_UNIDADE', 'CO_PROFISSIONAL_SUS', 'CO_CBO', 'CO_MUNICIPIO'])
    ## DATAWRANGLING - Trocando tipo de colunas
    df_final_2['CO_MUNICIPIO'] = df_final_2['CO_MUNICIPIO'].astype(str)
    df_final_2['CO_UNIDADE'] = df_final_2['CO_UNIDADE'].astype(str)
    ## AGRUPAMENTO - Agurpamento por CO_MUNICIPIO_GESTOR e renomeação de colunas
    df_final_2 = df_final_2.groupby(['CO_UNIDADE']).agg({'CO_PROFISSIONAL_SUS':'nunique'}).reset_index()
    df_final_2.columns = ['CO_UNIDADE','QTProfissionais']

    table = path_tables[-3]
    print(table)
    lista_colunas = [
        'CO_UNIDADE',
        'NU_CNPJ_ADM',
        "TO_CHAR(DT_VIGENCIA_INICIAL,'DD/MM/YYYY')",
        "TO_CHAR(DT_VIGENCIA_FINAL,'DD/MM/YYYY')",
    ]
    df_aux5 = pd.read_csv(table, usecols=lista_colunas, sep=';', encoding='utf-8')
    df_aux5['CO_UNIDADE'] = df_aux5['CO_UNIDADE'].astype(str)

    table = path_tables[-2]
    lista_colunas = [
        'CO_UNIDADE',
        'CO_CPF',
        'NO_REPRESENTANTE',
        'DS_CARGO',
        'DS_E_MAIL',
    ]
    df_aux6 = pd.read_csv(table, usecols=lista_colunas, sep=';', encoding='utf-8')
    try:
        df_aux6['CO_UNIDADE'] = df_aux6['CO_UNIDADE'].astype(str)
    except:
        print('err')

    #-------------------------UNIÃO DAS TABELAS DE INSIGHTS--------------------
    ## MERGE - DF_FINAL <- DF_FINAL_4 <- DF_FINAL_2 <- DF_FINAL_3
    df_final_final = df_final.merge(df_final_2, how='left', on='CO_UNIDADE')
    df_final_final = df_final_final.merge(df_aux5, how='left', on='CO_UNIDADE')
    df_final_final = df_final_final.merge(df_aux6, how='left', on='CO_UNIDADE')
    df_final_final['DT_DADOS'] = date
    ## DATAWRANGLING - Lidando com valores nulos
    ## VISUALIZANDO RESULTADOS

    return df_final_final

def save_on_final(df, path_final, date):
    df.to_csv(path_final+'/labs'+ date + '.csv', index=False)
    
if __name__ == "__main__":
    print('Running labs.py...')
    path_unziped, path_final, target_tables, date = recive_args()
    path_tables = [path_unziped + '/BASE_DE_/' + table + date + '.csv' for table in TARGET_TABLES]
    df = compile_info(path_tables, date)
    save_on_final(df, path_final, date)
    print('Script labs concluded')