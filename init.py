import os
import sys
import shutil
import requests
import subprocess
import threading
import concurrent.futures
import zipfile
import csv
import ta
from datetime import date
from dateutil.relativedelta import relativedelta

MONTHS = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
MONTHS_NUM = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
DAYS = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

LOG_ENABLED = os.getenv('LOG_DEV', False)

def log(message, error = False):
    if (error == True or LOG_ENABLED != False):
        print(message)


def remove_data_dir():
    log('Delete existing data: Starting')
    try:
        for folder_to_delete in ('nse_bhavcopy', 'bse_bhavcopy', 'data'):
            if(os.path.exists(folder_to_delete)):
                shutil.rmtree(folder_to_delete)
        if(os.path.exists('wget_log.log')):
            os.remove('wget_log.log')
        log('Delete existing data: Completed')
    except:
        log('Delete existing data: Failed', True)

def create_data_dir():
    try:
        remove_data_dir()
        log('Create required directories: Starting')
        for dir_to_create in ('bse_bhavcopy', 'nse_bhavcopy', 'data', 'data/bse', 'data/nse', 'data/ta_bse', 'data/ta_nse'):
            os.mkdir(dir_to_create)
        log('Create required directories: Completed')
    except:
        log('Create required directories: Failed', True)

def default_interval():
    return date.today() + relativedelta(days=-1) + relativedelta(months=-6)

def init_date_from_interval(unit, value):
    unit = unit.strip()
    if (unit != 'd' and unit != 'm' and unit != 'y'):
        return default_interval()
    
    try:
        value = int(value)
    except ValueError:
        return default_interval()
    
    if(unit == 'd'):
        return date.today() + relativedelta(days=-1) + relativedelta(days=-value)
    elif(unit == 'm'):
        return date.today() + relativedelta(days=-1) + relativedelta(months=-value)
    else:
        return date.today() + relativedelta(days=-1) + relativedelta(years=-value)

def return_init_date():
    if (len(sys.argv) == 1):
        return default_interval()
    
    interval = sys.argv[1].strip().split(':')
    
    if (len(interval) < 2):
        return default_interval()
    else:
        return init_date_from_interval(interval[0], interval[1])

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
    log('Download zipfile ' + filename + ': Starting')
    response = requests.get(url)
    
    if (response.status_code != 200):
        log('Download zipfile ' + filename + ': Failed (May be Holiday)')
        return

    with open(filename, 'wb') as zip_file:
        zip_file.write(response.content)
    
    log('Download zipfile ' + filename + ': Completed')

def download_zip_file_using_wget(url, filename):
    log('Download zipfile ' + filename + ': Starting')
    if (LOG_ENABLED != False):
        return_status = subprocess.call(['wget', '-O', filename, '-a', 'wget_log.log', url])
    else:
        return_status = subprocess.call(['wget', '-O', filename, '-q', url])

    if(return_status != 0):
        log('Download zipfile ' + filename + ': Failed (May be Holiday)')
        if(os.path.exists(filename)):
            log('Deleting trash file: ' + filename)
            os.remove(filename)
        return

    log('Download zipfile ' + filename + ': Completed')

def download_nse_bhavcopy(t_date):
    zip_file_name = get_nse_bhavcopy_filename(t_date)
    download_zip_file(get_nse_bhavcopy_url(t_date), 'nse_bhavcopy/'+zip_file_name)

def download_bse_bhavcopy(t_date):
    zip_file_name = get_bse_bhavcopy_filename(t_date)
    download_zip_file_using_wget(get_bse_bhavcopy_url(t_date), 'bse_bhavcopy/'+zip_file_name)

def download_historic_data():
    itr_date = return_init_date()
    curr_date = date.today()

    log('Download bhavcopy from ' + str(itr_date) + ' to ' + str(curr_date) + ': Starting')

    with concurrent.futures.ThreadPoolExecutor(50) as executor:
        futures = []
        while curr_date >= itr_date:
            itr_date += relativedelta(days=+1)
            
            if (itr_date.weekday() > 4):
                log('Download of bhavcopy for date ' + str(itr_date) + ' Skipped: (Holiday)')
                continue
            
            futures.append(executor.submit(download_nse_bhavcopy, t_date=itr_date))
            futures.append(executor.submit(download_bse_bhavcopy, t_date=itr_date))

    log('Download bhavcopy from ' + str(itr_date) + ' to ' + str(curr_date) + ': Completed')

def unzip_file(filename, target_dir):
    log('Extracting file ' + filename + ' to ' + target_dir)
    with zipfile.ZipFile(filename,'r') as zip_ref:
        zip_ref.extractall(target_dir)

def list_of_files(target_dir):
    return [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]

def list_of_dirs(target_dir):
    return [f for f in os.listdir(target_dir) if os.path.isdir(os.path.join(target_dir, f))]

def is_zip(filename):
    return filename.endswith('.zip') or filename.endswith('.ZIP')

def extract_all_files_in_dir(target_dir):
    log('Extracting all zip files in: ' + target_dir)
    for file_to_extract in list_of_files(target_dir):
        if (is_zip(file_to_extract)):
            unzip_file(target_dir+'/'+file_to_extract, target_dir)

def flatten_structure(curr_dir, root_dir):
    log('Flattenning directory structure for: ' + curr_dir)
    
    for dir_to_flatten in list_of_dirs(curr_dir):    
        for file_to_move in list_of_files(curr_dir+'/'+dir_to_flatten):
            shutil.move(curr_dir+'/'+dir_to_flatten+'/'+file_to_move, root_dir+'/temp_'+file_to_move)
        
        flatten_structure(curr_dir+'/'+dir_to_flatten, root_dir)
        
        log('Removing nested directory: ' + curr_dir+'/'+dir_to_flatten)
        shutil.rmtree(curr_dir+'/'+dir_to_flatten)

def flatten_nse_structure():
    flatten_structure('nse_bhavcopy', 'nse_bhavcopy')

    for filename in list_of_files('nse_bhavcopy'):
        if (filename.startswith('temp_')):
            actual_filename = filename[5:]
            log('Renaming temporary file nse_bhavcopy/' + filename + ' to actual filename ' + actual_filename)
            os.rename('nse_bhavcopy/'+filename, 'nse_bhavcopy/'+actual_filename)

def delete_zipfiles(target_dir):
    log('Deleting all zip files in ' + target_dir)
    for filename in list_of_files(target_dir):
        if (is_zip(filename)):
            os.remove(target_dir+'/'+filename)
            log('Zip File ' + target_dir+'/'+filename + ' deleted.')

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
    log('Processing csv: ' + filename)

    if (not os.path.exists(filename)):
        log('File ' + filename + ' not found. Skipping from CSV processing')
        return

    scripts = get_scripts('bse_scripts.dat')
    try:
        with open(filename, 'r') as f_handle:
            reader = csv.DictReader(f_handle)
            for row in reader:
                scripts[row['SC_CODE']] = row['SC_NAME']
                append_bse_script_data(row, t_date)
    except Exception as e:
        log('Failed to process csv data', True)
        log('Error: ' + e, True)
    
    write_scripts(scripts, 'bse_scripts.dat')

def generate_nse_data(t_date):
    filename = get_nse_csv_bhavcopy_filename(t_date)
    log('Processing csv: ' + filename)

    if (not os.path.exists(filename)):
        log('File ' + filename + ' not found. Skipping from CSV processing')
        return

    try:
        with open(filename, 'r') as f_handle:
            reader = csv.DictReader(f_handle)
            for row in reader:
                append_nse_script_data(row, t_date)
    except Exception as e:
        log('Failed to process csv data', True)
        log('Error: ' + e, True)

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
    log('Updating bse script details')
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
    t1 = threading.Thread(target=compute_bse_data)
    t2 = threading.Thread(target=compute_nse_data)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

def process_bse_data():
    itr_date = return_init_date()
    curr_date = date.today()

    log('Process BSE bhavcopy data: Starting')

    while curr_date >= itr_date:
        itr_date += relativedelta(days=+1)
        generate_bse_data(itr_date)

    log('Process BSE bhavcopy data: Completed')

def process_nse_data():
    itr_date = return_init_date()
    curr_date = date.today()

    log('Process NSE bhavcopy data: Starting')

    while curr_date >= itr_date:
        itr_date += relativedelta(days=+1)
        generate_nse_data(itr_date)

    log('Process NSE bhavcopy data: Completed')

def compute_bse_data():
    process_bse_bhavcopy()
    process_bse_data()
    process_ta('data/bse', 'data/ta_bse')

def compute_nse_data():
    process_nse_bhavcopy()
    process_nse_data()
    process_ta('data/nse', 'data/ta_nse')

def init():
    create_data_dir()
    download_historic_data()
    process_data()

def update_bhavcopy(t_date):
    t1 = threading.Thread(target=update_bse_bhavcopy, args=(t_date,))
    t2 = threading.Thread(target=update_nse_bhavcopy, args=(t_date,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

def update_nse_bhavcopy(t_date):
    download_nse_bhavcopy(t_date)
    bhavcopy_file = get_nse_bhavcopy_filename(t_date)
    process_nse_bhavcopy()
    generate_nse_data(t_date)
    process_ta('data/nse', 'data/ta_nse')

def update_bse_bhavcopy(t_date):
    download_bse_bhavcopy(t_date)
    bhavcopy_file = get_bse_bhavcopy_filename(t_date)
    process_bse_bhavcopy()
    generate_bse_data(t_date)
    process_ta('data/bse', 'data/ta_bse')

def fetch_and_process_today_data():
    t_date = date.today()
    update_bhavcopy(t_date)

def process_ta(target_dir, destination_dir):
    with concurrent.futures.ThreadPoolExecutor(80) as executor:
        futures = []
        for file_to_process in list_of_files(target_dir):
            destination_file = file_to_process[:-4] + "_TA.csv"
            log('Generating Technical Analysis data for ' + target_dir + '/' + file_to_process + ' in ' + destination_dir + '/' + destination_file)
            futures.append(executor.submit(ta.initialize_ta_data, datafile=target_dir+'/'+file_to_process, destination_file=destination_dir+'/'+destination_file))
    

    
if __name__ == '__main__':
    init()