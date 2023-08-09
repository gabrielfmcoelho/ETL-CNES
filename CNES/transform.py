def unzip_specific_files(path_temp, path_unziped, target_tables, date):
    """Unzip specific files from path_temp to path_unziped, according to target_tables"""
    import zipfile
    import os
    import regex as re
    for file in os.listdir(path_temp):
        if re.search(str(date), file):
            filename = file
    print(f'Unzipping {filename}')
    os.makedirs(f'{path_unziped}/BASE_DE_', exist_ok=True)
    existent_files = os.listdir(f'{path_unziped}/BASE_DE_')
    for table in target_tables:
        target = str(table)+str(date)+'.csv'
        if target in existent_files:
            continue
        with zipfile.ZipFile(f'{path_temp}/{filename}', 'r') as zip_ref:
            zip_ref.extract(target, path=f'{path_unziped}/BASE_DE_')
    return

def run_script(path_scripts, script, path_unziped, path_final, target_tables, date):
    """Run script from path_scripts"""
    print(f'Running script {script}...')
    import os
    import sys
    import subprocess

    script = f'{path_scripts}/{script}'
    print(script)

    subprocess.run(['python', script, path_unziped, path_final, str(date)])
    return

def compile_info(path_final, start_date, end_date):
    print('Compiling info...')
    import os
    import pandas as pd

    for count, file in enumerate(os.listdir(path_final)):
        df = pd.read_csv(f'{path_final}/{file}')
        df_merged = df if count == 0 else pd.concat([df_merged, df])
        os.unlink(f'{path_final}/{file}')

    filename_final = file[:4] + str(start_date) + '-' + str(end_date)
    try:
        df_merged.to_csv(
            f'{path_final}/{filename_final}.csv',
            sep=';',
            index=False,
            encoding='utf-8',
        )
    except:
        os.unlink(f'{path_final}/{filename_final}.csv')
        df_merged.to_csv(
            f'{path_final}/{filename_final}.csv',
            sep=';',
            index=False,
            encoding='utf-8',
        )
    
def move_and_clean(path_unziped, path_final, path_datasets):
    print('Moving the dataset and cleaning the temp files...')
    import shutil
    import os

    file = os.listdir(path_final)[0]
    try:
        shutil.move(f'{path_final}/{file}', path_datasets)
    except:
        os.unlink(f'{path_datasets}/{file}')
        shutil.move(f'{path_final}/{file}', path_datasets)

    for _ in os.listdir(path_unziped):
        shutil.rmtree(path_unziped)
    print('Cleaning complete')
    