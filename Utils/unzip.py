from zipfile import ZipFile
import re

class unzipfiles:
    def __init__(self, temp_dir, filename, temp_unziped, target_files, date):
        self.temp_dir = temp_dir
        self.filename = filename
        self.target_files = target_files
        self.temp_unziped = temp_unziped
        self.date = date

    def unzip_specific_basededadosCNES(self):
        for target_file in self.target_files:
            target = str(target_file)+str(self.date)+'.csv'
            with ZipFile(self.temp_dir+self.filename, 'r') as zObject:
                zObject.extract(target, path=self.temp_unziped+self.filename[0:8])

    def unzip_all(self):
        with ZipFile(self.temp_dir+self.filename, 'r') as zObject:
            zObject.extractall(path=self.temp_unziped+self.filename[0:8])