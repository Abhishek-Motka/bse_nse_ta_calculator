import csv
import os

CSV_FIELDS = ['date', 'open', 'high', 'low', 'close', 'volume']
TA_CSV_FIELDS = ['date', 'UP', 'DOWN', 'RSI', 'EMA_50', 'EMA_21', 'EMA_9', 'MACD', 'MACD_SIG', 'VOL_EMA', 'P_CHANGE']

LOG_ENABLED = os.getenv('LOG_DEV', False)

def log(message, error = False):
    if (error == True or LOG_ENABLED != False):
        print(message)

def rsi(datafile, n):
    log('Calculating RSI for ' + datafile)
    data = {}
    
    with open(datafile, 'r') as f_handle:
        reader = csv.DictReader(f_handle, CSV_FIELDS)
        sum_up = 0
        sum_down = 0
        
        for i, row in enumerate(reader):
            convert_values_to_float(row)
            index = str(i)
            data[index] = {}
            data[index]['date'] = row['date']
            
            if (row['open'] >= row['close']):
                data[index]['UP'] = 0
                data[index]['DOWN'] = round(row['open'] - row['close'], 4)
            else:
                data[index]['UP'] = round(row['close'] - row['open'], 4)
                data[index]['DOWN'] = 0
            
            sum_up += data[index]['UP']
            sum_down += data[index]['DOWN']
            data[index]['RSI'] = 'NA'
            
            if (i == (n-1)):
                if (sum_down <= 0.001):
                    data[index]['RSI'] = 100
                else:
                    data[index]['RSI'] = round(100 - (100/(1+(sum_up/sum_down))), 4)
            elif (i >= n):
                sum_up -= data[str(i-n)]['UP']
                sum_down -= data[str(i-n)]['DOWN']
                if (sum_down <= 0.001):
                    data[index]['RSI'] = 100
                else:
                    data[index]['RSI'] = round(100 - (100/(1+(sum_up/sum_down))), 4)
    
    log('RSI calculation completed for ' + datafile)
    return data


def moving_average(datafile, field, n):
    log('Calculating ' + str(n) + ' days Exponential Moving Average for ' + field + ' field')
    data = {}
    
    with open(datafile, 'r') as f_handle:
        reader = csv.DictReader(f_handle, CSV_FIELDS)
        sum_field = 0

        for i, row in enumerate(reader):
            convert_values_to_float(row)
            index = str(i)
            data[index] = {}
            sum_field += row[field]
            data[index]['date'] = row['date']
            
            if (i == (n-1)):
                data[index]['EMA'] = round(sum_field/n, 4)
            elif (i >= n):
                data[index]['EMA'] = round((row[field]*(2/(1+n)))+(data[str(i-1)]['EMA']*(1 - (2/(n+1)))), 4)
            else:
                data[index]['EMA'] = 'NA'
                
    log('Calculation of ' + str(n) + ' days Exponential Moving Average for ' + field + ' field: Completed')
    return data


def convert_values_to_float(csv_row):
    csv_row['open'] = i2f(csv_row['open'])
    csv_row['close'] = i2f(csv_row['close'])
    csv_row['high'] = i2f(csv_row['high'])
    csv_row['low'] = i2f(csv_row['low'])
    csv_row['volume'] = int(csv_row['volume'])


def macd(datafile, low_n, high_n, signal):
    log('Calculating MACD('+str(high_n)+', '+str(low_n)+', '+str(signal)+') for ' + datafile)
    if (low_n > high_n):
        high_n += low_n
        low_n = high_n - low_n
        high_n -= low_n

    ma_low = moving_average(datafile, 'close', low_n)
    ma_high = moving_average(datafile, 'close', high_n)
    i = 0;
    data = {}

    while(i < len(ma_high)):
        index = str(i)
        data[index] = {}
        
        data[index]['date'] = ma_low[index]['date']
        
        if (i < (high_n - 1)):
            data[index]['MACD'] = 'NA'
        else:
            data[index]['MACD'] = round(ma_low[index]['EMA'] - ma_high[index]['EMA'], 4)

        i += 1
    
    log('Calculation of MACD('+str(high_n)+', '+str(low_n)+', '+str(signal)+') for ' + datafile + ' Completed')
    return calculate_macd_signal(data, high_n, signal)
    

def calculate_macd_signal(data, high_n, signal):
    log('Generating signal data for previously computed MACD data')
    i = 0
    sum_macd = 0
    
    while i < len(data):
        index = str(i)
        
        if (i < (high_n - 1)):
            data[index]['MACD_SIG'] = 'NA'
        elif (i < (high_n + signal - 2)):
            data[index]['MACD_SIG'] = 'NA'
            sum_macd += data[index]['MACD']
        elif (i == (high_n + signal - 2)):
            sum_macd += data[index]['MACD']
            data[index]['MACD_SIG'] = round(sum_macd/signal, 4)
        else:
            sum_macd += data[index]['MACD'] - data[str(i - signal)]['MACD']
            data[index]['MACD_SIG'] = round(sum_macd/signal, 4)

        i += 1

    log('Signal data for previously computed MACD data generated')
    return data


def percent_change(datafile):
    log('Calculating %change for ' + datafile)
    with open(datafile, 'r') as f_handle:
        reader = csv.DictReader(f_handle, CSV_FIELDS)
        prev_close = -1
        data = {}

        for i, row in enumerate(reader):
            convert_values_to_float(row)
            index = str(i)
            data[index] = {}
            data[index]['date'] = row['date']
            
            if (i == 0):
                prev_close = row['open']
            
            data[index]['PCHANGE'] = round(100*(row['close'] - prev_close)/prev_close, 4)
            prev_close = row['close']

        log('Calculation completed for %change for ' + datafile)
        return data


def calculate_ta(datafile):
    log('Starting calculation of Technical Analysis data for ' + datafile)
    rsi_14 = rsi(datafile, 14)
    ma_50 = moving_average(datafile, 'close', 50)
    ma_21 = moving_average(datafile, 'close', 21)
    ma_9 = moving_average(datafile, 'close', 9)
    mcd = macd(datafile, 12, 26, 9)
    vma_10 = moving_average(datafile, 'volume', 10)
    pchange = percent_change(datafile)

    data = {}
    for index in pchange:
        data[index] = rsi_14[index]
        data[index]['EMA_50'] = ma_50[index]['EMA']
        data[index]['EMA_21'] = ma_21[index]['EMA']
        data[index]['EMA_9'] = ma_9[index]['EMA']
        data[index]['MACD'] = mcd[index]['MACD']
        data[index]['MACD_SIG'] = mcd[index]['MACD_SIG']
        data[index]['VOL_EMA'] = vma_10[index]['EMA']
        data[index]['P_CHANGE'] = pchange[index]['PCHANGE']

    log('Calculation of Technical Analysis data completed for ' + datafile)
    return data


def write_ta_data_to_file(data, destination_file):
    log('Writing Technical Analysis data to ' + destination_file)
    with open(destination_file, 'w') as f_handle:
        writer = csv.DictWriter(f_handle, TA_CSV_FIELDS)
        writer.writeheader()
        for index in data:
            writer.writerow(data[index])


def initialize_ta_data(datafile, destination_file):
    data = calculate_ta(datafile)
    write_ta_data_to_file(data, destination_file)


def i2f(value):
    return round(float(value), 4)