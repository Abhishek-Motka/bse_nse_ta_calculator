import os
import sys
import shutil
import requests
import subprocess
import zipfile
import csv
import ta
from datetime import date
from dateutil.relativedelta import relativedelta

MONTHS = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
MONTHS_NUM = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
DAYS = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

def remove_data_dir():
    print('Delete existing data: Starting')
    try:
        if(os.path.exists('nse_bhavcopy')):
            shutil.rmtree('nse_bhavcopy')
        if(os.path.exists('bse_bhavcopy')):
            shutil.rmtree('bse_bhavcopy')
        if(os.path.exists('data')):
            shutil.rmtree('data')
        if(os.path.exists('wget_log.log')):
            os.remove('wget_log.log')
        print('Delete existing data: Completed')
    except:
        print('Delete existing data: Failed')

def create_data_dir():
    try:
        remove_data_dir()
        print('Create required directories: Starting')
        os.mkdir('bse_bhavcopy')
        os.mkdir('nse_bhavcopy')
        os.mkdir('data')
        os.mkdir('data/bse')
        os.mkdir('data/nse')
        os.mkdir('data/ta_nse')
        os.mkdir('data/ta_bse')
        print('Create required directories: Completed')
    except:
        print('Create required directories: Failed')

def return_init_date():
    return date.today() + relativedelta(months=-6) + relativedelta(days=-1);

def get_nse_bhavcopy_filename(t_date):
    month = MONTHS[t_date.month]
    year = str(t_date.year)
    day = DAYS[t_date.day]
    return 'cm'+day+month+year+'bhav.csv.zip'

def get_bse_bhavcopy_filename(t_date):
    month = MONTHS_NUM[t_date.month]
    year = str(t_date.year % 2000)
    day = DAYS[t_date.day]
    return 'EQ'+day+month+year+'_CSV.ZIP'

def get_nse_bhavcopy_url(t_date):
    month = MONTHS[t_date.month]
    year = str(t_date.year)
    return 'https://www1.nseindia.com/content/historical/EQUITIES/'+year+'/'+month+'/'+get_nse_bhavcopy_filename(t_date);

def get_bse_bhavcopy_url(t_date):
    return 'https://www.bseindia.com/download/BhavCopy/Equity/'+get_bse_bhavcopy_filename(t_date)

def download_zip_file(url, filename):
    print('Download zipfile ' + filename + ': Starting')
    response = requests.get(url)
    
    if (response.status_code != 200):
        print('Download zipfile ' + filename + ': Failed (May be Holiday)')
        return

    with open(filename, 'wb') as zip_file:
        zip_file.write(response.content)
    
    print('Download zipfile ' + filename + ': Completed')

def download_zip_file_using_wget(url, filename):
    print('Download zipfile ' + filename + ': Starting')
    return_status = subprocess.call(['wget', '-O', filename, '-a', 'wget_log.log', url])

    if(return_status != 0):
        print('Download zipfile ' + filename + ': Failed (May be Holiday)')
        if(os.path.exists(filename)):
            print('Deleting trash file: ' + filename)
            os.remove(filename)
        return

    print('Download zipfile ' + filename + ': Completed')

def download_nse_bhavcopy(t_date):
    zip_file_name = get_nse_bhavcopy_filename(t_date)
    download_zip_file(get_nse_bhavcopy_url(t_date), 'nse_bhavcopy/'+zip_file_name)

def download_bse_bhavcopy(t_date):
    zip_file_name = get_bse_bhavcopy_filename(t_date)
    download_zip_file_using_wget(get_bse_bhavcopy_url(t_date), 'bse_bhavcopy/'+zip_file_name)

def download_6m_historic_data():
    itr_date = return_init_date()
    curr_date = date.today()

    print('Download bhavcopy from ' + str(itr_date) + ' to ' + str(curr_date) + ': Starting')

    while curr_date >= itr_date:
        itr_date += relativedelta(days=+1)
        
        if (itr_date.weekday() > 4):
            print('Download of bhavcopy for date ' + str(itr_date) + ' Skipped: (Holiday)')
            continue
        
        download_nse_bhavcopy(itr_date)
        download_bse_bhavcopy(itr_date)

    print('Download bhavcopy from ' + str(itr_date) + ' to ' + str(curr_date) + ': Completed')

def unzip_file(filename, target_dir):
    print('Extracting file ' + filename + ' to ' + target_dir)
    with zipfile.ZipFile(filename,'r') as zip_ref:
        zip_ref.extractall(target_dir)

def list_of_files(target_dir):
    return [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

def list_of_dirs(target_dir):
    return [f for f in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, f))]

def is_zip(filename):
    return filename.endswith('.zip') or filename.endswith('.ZIP')

def extract_all_files_in_dir(target_dir):
    print('Extracting all zip files in: ' + target_dir)
    for file_to_extract in list_of_files(target_dir):
        if (is_zip(file_to_extract)):
            unzip_file(target_dir+'/'+file_to_extract, target_dir)

def flatten_structure(curr_dir, root_dir):
    print('Flattenning directory structure for: ' + curr_dir)
    
    for dir_to_flatten in list_of_dirs(curr_dir):    
        for file_to_move in list_of_files(curr_dir+'/'+dir_to_flatten):
            shutil.move(curr_dir+'/'+dir_to_flatten+'/'+file_to_move, root_dir+'/temp_'+file_to_move)
        
        flatten_structure(curr_dir+'/'+dir_to_flatten, root_dir)
        
        print('Removing nested directory: ' + curr_dir+'/'+dir_to_flatten)
        shutil.rmtree(curr_dir+'/'+dir_to_flatten)

def flatten_nse_structure():
    flatten_structure('nse_bhavcopy', 'nse_bhavcopy')

    for filename in list_of_files('nse_bhavcopy'):
        if (filename.startswith('temp_')):
            actual_filename = filename[5:]
            print('Renaming temporary file nse_bhavcopy/' + filename + ' to actual filename ' + actual_filename)
            os.rename('nse_bhavcopy/'+filename, 'nse_bhavcopy/'+actual_filename)

def delete_zipfiles(target_dir):
    print('Deleting all zip files in ' + target_dir)
    for filename in list_of_files(target_dir):
        if (is_zip(filename)):
            os.remove(target_dir+'/'+filename)
            print('Zip File ' + target_dir+'/'+filename + ' deleted.')

def process_nse_bhavcopy():
    extract_all_files_in_dir('nse_bhavcopy')
    flatten_nse_structure()
    delete_zipfiles('nse_bhavcopy')

def process_bse_bhavcopy():
    extract_all_files_in_dir('bse_bhavcopy')
    delete_zipfiles('bse_bhavcopy')

def get_bse_csv_bhavcopy_filename(t_date):
    month = MONTHS_NUM[t_date.month]
    year = str(t_date.year % 2000)
    day = DAYS[t_date.day]
    return 'bse_bhavcopy/EQ'+day+month+year+'.CSV'

def get_nse_csv_bhavcopy_filename(t_date):
    month = MONTHS[t_date.month]
    year = str(t_date.year)
    day = DAYS[t_date.day]
    return 'nse_bhavcopy/cm'+day+month+year+'bhav.csv'

def generate_bse_data(t_date):
    filename = get_bse_csv_bhavcopy_filename(t_date)
    print('Processing csv: ' + filename)

    if (not os.path.exists(filename)):
        print('File ' + filename + ' not found. Skipping from CSV processing')
        return

    scripts = get_scripts('bse_scripts.dat')
    try:
        with open(filename, 'r') as f_handle:
            reader = csv.DictReader(f_handle)
            for row in reader:
                scripts[row['SC_CODE']] = row['SC_NAME']
                append_bse_script_data(row, t_date)
    except Exception as e:
        print('Failed to process csv data')
        print('Error: ' + e)
    
    write_scripts(scripts, 'bse_scripts.dat')

def generate_nse_data(t_date):
    filename = get_nse_csv_bhavcopy_filename(t_date)
    print('Processing csv: ' + filename)

    if (not os.path.exists(filename)):
        print('File ' + filename + ' not found. Skipping from CSV processing')
        return

    try:
        with open(filename, 'r') as f_handle:
            reader = csv.DictReader(f_handle)
            for row in reader:
                append_nse_script_data(row, t_date)
    except Exception as e:
        print('Failed to process csv data')
        print('Error: ' + e)

def append_bse_script_data(csv_row, t_date):
    if(csv_row['SC_TYPE'] != 'Q'):
        return

    with open('data/bse/'+csv_row['SC_CODE']+'.csv', 'a') as f_handle:
        writer = csv.writer(f_handle)
        writer.writerow([str(t_date), csv_row['OPEN'], csv_row['HIGH'], csv_row['LOW'], csv_row['CLOSE'], csv_row['NO_OF_SHRS']])

def append_nse_script_data(csv_row, t_date):
    if(csv_row['SERIES'] != 'EQ'):
        return

    with open('data/nse/'+csv_row['SYMBOL']+'.csv', 'a') as f_handle:
        writer = csv.writer(f_handle)
        writer.writerow([str(t_date), csv_row['OPEN'], csv_row['HIGH'], csv_row['LOW'], csv_row['CLOSE'], csv_row['TOTTRDQTY']])
    
def write_scripts(scripts, script_file):
    fields = ['code', 'name']
    print('Updating bse script details')
    with open('data/'+script_file, 'w') as f_handle:
        writer = csv.DictWriter(f_handle, fields)
        for key in scripts:
            writer.writerow({'code': key, 'name': scripts[key]})

def get_scripts(script_file):
    scripts = {}
    try:
        with open('data/'+script_file, 'r') as f_handle:
            reader = csv.DictReader(f_handle, ['code', 'name'])
            for row in reader:
                scripts[row['code']] = row['name']
        return scripts
    except Exception as e:
        return scripts

def process_data():
    itr_date = return_init_date()
    curr_date = date.today()

    print('Process BSE bhavcopy data: Starting')

    while curr_date >= itr_date:
        itr_date += relativedelta(days=+1)
        process_bse_data(itr_date)
        process_nse_data(itr_date)

    print('Process BSE bhavcopy data: Completed')

def process_bse_data(t_date):
    generate_bse_data(t_date)

def process_nse_data(t_date):
    generate_nse_data(t_date)

def init():
    create_data_dir()
    download_6m_historic_data()
    process_nse_bhavcopy()
    process_bse_bhavcopy()
    process_data()
    process_ta('data/bse', 'data/ta_bse')
    process_ta('data/nse', 'data/ta_nse')


def process_ta(target_dir, destination_dir):
    for file_to_process in list_of_files(target_dir):
        destination_file = file_to_process[:-4] + "_TA.csv"
        print('Generating Technical Analysis data for ' + target_dir + '/' + file_to_process + ' in ' + destination_dir + '/' + destination_file)
        ta.initialize_ta_data(target_dir+'/'+file_to_process, destination_dir+'/'+destination_file)
    