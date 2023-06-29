import os
import ftplib

def verify_integrity_and_existence(ftp_host, ftp_path, mode, start_date, end_date, files, temp_folder):
    """Verify if the files in the temp folder are the same as the files in the ftp path and if size is the same."""
    print('Verifying local files integrity and existence...')
    if mode == 'interval':
        files_to_download = [files[0] + str(date) + '.ZIP' for date in range(start_date, end_date + 1)]
    else:
        files_to_download = []
    files_to_keep = []
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(ftp_path)
        for filename in files_to_download:
            file_path = os.path.join(temp_folder, filename)
            try:
                if os.path.isfile(file_path):
                    if ftp.size(filename) == os.path.getsize(file_path):
                        files_to_keep.append(filename)
                    else:
                        os.unlink(file_path)
            except Exception as e:
                print(e)
    print(f'{len(files_to_keep)} OK files found')
    print(f'{len(files_to_download) - len(files_to_keep)} files to download')
    return files_to_keep 

def clean_temp_folder(files_to_keep, temp_folder):
    """Delete all files in the temp folder except the files that should be kept."""
    print('Cleaning temp folder...')
    for filename in os.listdir(temp_folder):
        file_path = os.path.join(temp_folder, filename)
        try:
            if os.path.isfile(file_path) and filename not in files_to_keep:
                os.unlink(file_path)
        except Exception as e:
            print(e)
    print('Temp folder cleaned')

def list_ftp_path(ftp_host, path):
    """List all files in the ftp path."""
    print('Listing ftp path...')
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(path)
        files_in_path = ftp.nlst()
    print(f'{len(files_in_path)} files found')
    return files_in_path

def filter_files(files_in_path, mode, start_date, end_date, files, files_to_keep):
    """Filter files by mode and date."""
    print('Filtering files...')
    if mode == 'interval':
        files_to_download = [files[0] + str(date) + '.ZIP' for date in range(start_date, end_date + 1)]
        filtered_files = [file for file in files_in_path if file in files_to_download and file not in files_to_keep]
    else:
        filtered_files = []
    print(f'{len(filtered_files)} files with matching criteria')
    return filtered_files

def download_files(ftp_host, ftp_path, filtered_files, temp_folder):
    """Download all filtered files."""
    print(f'Downloading {len(filtered_files)} files...')
    import Utils.progressbar as pg
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(ftp_path)
        count = 1
        for file in filtered_files:
            file_size = ftp.size(file)
            progress = pg.AnimatedProgressBar(end=file_size, width=50)
            print(f'{count}/{len(filtered_files)} Downloading {file}...')
            with open(os.path.join(temp_folder, file), 'wb') as f:
                def callback(chunk):
                    f.write(chunk)
                    progress + len(chunk)
                    progress.show_progress()
                ftp.retrbinary('RETR ' + file, callback)
                print('')
            count += 1
    print('Files downloaded')
    return