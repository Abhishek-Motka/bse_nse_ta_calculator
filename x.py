import sys
from datetime import date
from dateutil.relativedelta import relativedelta

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

if __name__ == '__main__':
	print(return_init_date())
