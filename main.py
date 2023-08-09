DB_CREDENTIAL_PATH = 'etl-cnes-a7a20431c4d7.json'

TASK_CNES_UIDS = ['default']
TASK_SIDRA_UIDS = []
TASK_SIH_UIDS = []
TASK_MERGE_UIDS = ['default', 'labs']
TASK_LOAD_TO_DB_UIDS = ['default', 'labs']

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

TASK_OPTIONS = {
    'run_cnes': {
        'run': False,
        'task_uid': 'default',
        'extract': {
            'run': True,
            'options': {
                'ftp_host': 'ftp.datasus.gov.br',
                'ftp_path': 'cnes',
                'mode': 'interval',
                'start_date': 202201,
                'end_date': 202212,
                'files': ['BASE_DE_DADOS_CNES_'],
            },
        },
        'transform': {
            'run': False,
            'options': {
                'target_tables': TARGET_TABLES,
                'script_data_engineering': 'labs.py',
            },
        },
        'load': {
            'run': False,
            'options': {
            },
        },
    },
    'run_sidra': {
        'run': False,
        'task_uid': '',
        'extract': {
            'run': False,
            'options': {
            },
        },
        'transform': {
            'run': False,
            'options': {
            },
        },
        'load': {
            'run': False,
            'options': {
            },
        },
    },
    'run_sih': {
        'run': False,
        'task_uid': '',
        'extract': {
            'run': False,
            'options': {
            },
        },
        'transform': {
            'run': False,
            'options': {
            },
        },
        'load': {
            'run': False,
            'options': {
            },
        },
    },
    'run_merge': {
        'run': True,
        'task_uid': 'labs',
        'options': {
        },
    },
    'run_load_to_db': {
        'run': False,
        'task_uid': 'labs',
        'options': {
        },
    },
}

if __name__ == '__main__':
    print('RUNNING ETL SOLUDE GROWTH PIPELINE...')
    from CNES.etl import etl
    import subprocess
    cnes_etl = etl(TASK_OPTIONS['run_cnes'], DB_CREDENTIAL_PATH)
    cnes_etl.run()

    if TASK_OPTIONS['run_merge']['run']:
        script = './DataEngineering' + '/' + TASK_OPTIONS['run_merge']['task_uid'] + '.py'
        subprocess.run(['python', script, './Datasets'])