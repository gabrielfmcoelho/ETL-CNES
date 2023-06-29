import os

PATH_TEMP = './CNES/Temp'
PATH_UNZIPED = './CNES/Unziped'
PATH_FINAL = './CNES/Final'
PATH_SCRIPTS = './CNES/DataEngineering'
PATH_DATASETS = './Datasets'

class etl():
    def __init__(self, task_parameters, db_credential_path):
        self.should_run = task_parameters['run']
        self.task_parameters = task_parameters
        self.date_interval = [i for i in range(task_parameters['extract']['options']['start_date'], task_parameters['extract']['options']['end_date']+1)]
        self.db_credential = db_credential_path

    def extract(self, options):
        print('Extracting data from CNES...')
        import CNES.extract as extract
        files_to_keep = extract.verify_integrity_and_existence(options['ftp_host'], options['ftp_path'], options['mode'], options['start_date'], options['end_date'], options['files'], PATH_TEMP)
        extract.clean_temp_folder(files_to_keep, PATH_TEMP)

        files_in_path = extract.list_ftp_path(options['ftp_host'], options['ftp_path'])
        filtered_files = extract.filter_files(files_in_path, options['mode'], options['start_date'], options['end_date'], options['files'], files_to_keep)

        extract.download_files(options['ftp_host'], options['ftp_path'], filtered_files, PATH_TEMP)
        return
    
    def transform(self, options):
        print('Transforming data from CNES...')
        import CNES.transform as transform
        for date in self.date_interval:
            transform.unzip_specific_files(PATH_TEMP, PATH_UNZIPED, options['target_tables'], date)

        for date in self.date_interval:
            for script in os.listdir(PATH_SCRIPTS):
                if script == options['script_data_engineering']:
                    transform.run_script(PATH_SCRIPTS, script, PATH_UNZIPED, PATH_FINAL, options['target_tables'], date)

        transform.compile_info(PATH_FINAL, min(self.date_interval), max(self.date_interval))
        transform.move_and_clean(PATH_UNZIPED, PATH_FINAL, PATH_DATASETS)
        return 
    
    def load(self, options):
        print('Loading data from CNES...')
        return 

    def run(self):
        if self.should_run:
            print('Running ETL CNES')
            if self.task_parameters['extract']['run']:
                self.extract(self.task_parameters['extract']['options'])
            if self.task_parameters['transform']['run']:
                self.transform(self.task_parameters['transform']['options'])
            if self.task_parameters['load']['run']:
                self.load(self.task_parameters['load']['options'])
        else:
            print('Skipping ETL CNES')
