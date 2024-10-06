import datetime

def format_date_german(datum):
    months = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 
              'August', 'September', 'Oktober', 'November', 'Dezember']
    return f"{datum.day}. {months[datum.month-1]} {datum.year}"