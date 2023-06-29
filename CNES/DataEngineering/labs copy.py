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
    df_final = df_main.drop(df_main[(df_main['CO_TIPO_ESTABELECIMENTO'] != '39') & (df_main['TP_UNIDADE'] != '67') & (df_main['TP_UNIDADE'] != '80')].index).copy()

    ## RLESTABCOMPLEMENTAR - tabela de dados quantitativos de leitos
    table = path_tables[5]
    lista_colunas = [
        'CO_UNIDADE',
        'CO_LEITO',
        'CO_TIPO_LEITO',
        'QT_EXIST',
        'QT_SUS',
    ]
    df_aux = pd.read_csv(table, sep=';', encoding='utf-8', usecols=lista_colunas)
    ## DATAWRANGLING - Trocando tipo de colunas
    df_aux['CO_UNIDADE'] = df_aux['CO_UNIDADE'].astype(str)
    ## AGRUPAMENTO - Agrupamento por CO_UNIDADE e CO_LEITO
    df_aux = df_aux.groupby(['CO_UNIDADE']).agg({'QT_EXIST': 'sum', 'QT_SUS':'sum'}).reset_index().sort_values(by=['CO_UNIDADE'])
    ## MERGE - DF_FINAL >-< RLESTABCOMPLEMENTAR (inner)
    df_final = df_final.merge(df_aux, how='inner', on='CO_UNIDADE')

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
    df_aux = df_aux.drop(df_aux[(df_aux['CO_CONVENIO'] != '3') & (df_aux['CO_CONVENIO'] != '4') & (df_aux['CO_CONVENIO'] != '5') & (df_aux['CO_CONVENIO'] != '6')].index)
    ## MERGE - DF_FINAL <- TBATENDIMENTOPRESTADO (left)
    df_final = df_final.merge(df_aux, how='left', on='CO_UNIDADE')
    
    ## AGRUPAMENTO - Agurpamento por CO_MUNICIPIO_GESTOR
    df_final['QT_NaoSUS'] = df_final['QT_EXIST'] - df_final['QT_SUS']
    df_final = df_final.groupby(['CO_MUNICIPIO_GESTOR']).agg({'QT_EXIST': 'sum', 'QT_SUS':'sum', 'QT_NaoSUS':'sum'}).reset_index().sort_values(by=['CO_MUNICIPIO_GESTOR'])
    df_final.columns = ['CO_MUNICIPIO_GESTOR','QTLeitosExist','QTLeitosSUS','QT_NaoSUS']

    #-------------------------CONTABILIZAÇÃO DE PROFISSIONAIS DA SAUDE---------
    table = path_tables[12]
    df_final_2 = pd.read_csv(table, sep=';', encoding='utf-8', usecols=['CO_UNIDADE', 'CO_PROFISSIONAL_SUS', 'CO_CBO', 'CO_MUNICIPIO'])
    ## DATAWRANGLING - Trocando tipo de colunas
    df_final_2['CO_MUNICIPIO'] = df_final_2['CO_MUNICIPIO'].astype(str)
    ## AGRUPAMENTO - Agurpamento por CO_MUNICIPIO_GESTOR e renomeação de colunas
    df_final_2 = df_final_2.groupby(['CO_MUNICIPIO']).agg({'CO_PROFISSIONAL_SUS':'nunique'}).reset_index()
    df_final_2.columns = ['CO_MUNICIPIO_GESTOR','QTProfissionais']

    #-------------------------CONTABILIZAÇÃO DE ESTABELECIMENTOS DA SAUDE------
    # RLESTABCOMPLEMENTAR - tabela de dados quantitativos de leitos
    table = path_tables[5]
    lista_colunas = [
        'CO_UNIDADE',
        #'CO_LEITO',
        'CO_TIPO_LEITO',
    ]
    df_aux = pd.read_csv(table, sep=';', encoding='utf-8', usecols=lista_colunas)
    df_aux['CO_UNIDADE'] = df_aux['CO_UNIDADE'].astype(str)
    df_aux['CO_TIPO_LEITO'] = df_aux['CO_TIPO_LEITO'].astype(str)
    ## MERGE - DF_MAIN <- RLESTABCOMPLEMENTAR (Left)
    df_aux = df_main.merge(df_aux, how='left', on='CO_UNIDADE')
    ## AGRUPAMENTO - Agurpamento por CO_UNIDADE e CO_TIPO_LEITO
    def possui_leito_pediatrico(x):
        #se coluna ['CO_TIPO_LEITO'] possuir 5 return 1 se não return 0
        if '5' in x.values:
            return 1
        else:
            return 0
    df_aux = df_aux.groupby(['CO_MUNICIPIO_GESTOR']).agg({'CO_TIPO_LEITO':['nunique',possui_leito_pediatrico]}).reset_index()
    df_aux.columns = ['CO_MUNICIPIO_GESTOR','DVLeitos','EXTLeitosPediatrico']

    ## DATAWRANGLING - Trocando tipo de colunas, Criando Tipos de Unidades Agrupados, Eliminando CO_UNIDADE duplicados
    df_final_3 = df_main.drop_duplicates(['CO_UNIDADE'])
    df_final_3['TP_UNIDADE'] = df_final_3['TP_UNIDADE'].astype('int64')
    def agrupar_Unidades(x):
        if x == 5 or x == 7 or x == 62:
            return 1
        elif x == 4 or x == 36:
            return 2
        elif x == 39:
            return 3
        elif x == 22:
            return 4
        elif x == 1 or x == 2:
            return 5
    df_final_3['TP_UNIDADE_AGRUPADO'] = df_final_3['TP_UNIDADE'].apply(agrupar_Unidades)

    ## AGRUPAMENTO - Agrupamento por CO_MUNICIPIO_GESTOR e renomeando colunas
    def contar_hopitais(x):
        return len(x.loc[x == 1])
    def contar_clinicas(x):
        return len(x.loc[x == 2])
    def contar_laboratorios(x):
        return len(x.loc[x == 3])
    def contar_consultorios(x):
        return len(x.loc[x == 4])
    def contar_unidades_basicas(x):
        return len(x.loc[x == 5])
    df_final_3 = df_final_3.groupby(['CO_MUNICIPIO_GESTOR']).agg({'CO_CNES':'nunique','CO_UNIDADE':'nunique','TP_UNIDADE':'nunique','CO_TIPO_UNIDADE':'nunique','CO_TIPO_ESTABELECIMENTO':'nunique','TP_UNIDADE_AGRUPADO':[contar_hopitais,contar_clinicas,contar_laboratorios, contar_consultorios, contar_unidades_basicas]}).reset_index()
    df_final_3.columns = ['CO_MUNICIPIO_GESTOR','QTCNES','QTUnidades','DVtpunidade','DVcotpunidade','DVcotpestab','QTHospitais','QTClinicas','QTLaboratorios', 'QTConsultorios', 'QTUnidadesBasicas']

    ## MERGE - DF_FINAL_3 <- DF_AUX (left)
    df_final_3 = df_final_3.merge(df_aux, how='left', on='CO_MUNICIPIO_GESTOR')

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
    df_final_final = df_final.merge(df_final_2, how='left', on='CO_MUNICIPIO_GESTOR')
    df_final_final = df_final_final.merge(df_final_3, how='left', on='CO_MUNICIPIO_GESTOR')
    df_final_final = df_final_final.merge(df_aux5, how='left', on='CO_UNIDADE')
    df_final_final = df_final_final.merge(df_aux6, how='left', on='CO_UNIDADE')
    df_final_final['DT_DADOS'] = date
    ## DATAWRANGLING - Lidando com valores nulos
    df_final_final['QTLeitosExist'] = df_final_final['QTLeitosExist'].fillna(0)
    df_final_final['QTLeitosSUS'] = df_final_final['QTLeitosSUS'].fillna(0)
    df_final_final['QT_NaoSUS'] = df_final_final['QT_NaoSUS'].fillna(0)
    df_final_final['QTProfissionais'] = df_final_final['QTProfissionais'].fillna(0)
    df_final_final['QTCNES'] = df_final_final['QTCNES'].fillna(0)
    df_final_final['QTUnidades'] = df_final_final['QTUnidades'].fillna(0)
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