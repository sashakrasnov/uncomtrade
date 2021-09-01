'''
This module performs bulk loading into SQL database which is much faster
than execute INSERT query many times
'''


import os, json
import pandas as pd
from datetime import date
from sqlalchemy import create_engine


def none_conv(v):
    return v if v is not None else 0

# These unnecessary columns in csv-file would be dropped later just before saving
csv_cols = (
    'Period Desc.',
    'Trade Flow',
    'Reporter',
    'Reporter ISO',
    'Partner',
    'Partner ISO',
    '2nd Partner Code',
    '2nd Partner',
    '2nd Partner ISO',
    'Customs Proc. Code',
    'Customs',
    'Mode of Transport Code',
    'Mode of Transport',
    'Commodity',
    'Qty Unit',
    'Alt Qty Unit Code',
    'Alt Qty Unit',
    'Alt Qty',
    'Flag'
)

# Log filename. It keeps the information about what was managed to process
log_file = 'db_update.log'

# Update period
last_year = date.today().year - 3
years = range(last_year, 2014, -1)

# Skip already processed to the new
with open(log_file, 'r') as db_f:
    files_done = [x.strip() for x in list(db_f)] 

# Database connection
engine = create_engine('mysql://root:zxcvbn123@localhost:3306/uncomtrade')

with engine.connect() as con:
    for year in years:
        folder = './' + str(year) + '/'

        with os.scandir(folder) as dir:
            for fl in dir:
                file_name = folder + fl.name

                if not fl.name.startswith('.') and fl.is_file() and file_name not in files_done:
                    print(file_name)

                    execute = False

                    # JSON files to load
                    if file_name[-4:] == 'json':
                     
                        with open(file_name, 'r', encoding='utf-8') as f:
                            j = json.load(f)

                        bulks = ''

                        # Only non-zero values
                        if j['validation']['count']['value'] > 0:

                            for r in j['dataset']:
                                # Doesn't need totals
                                if r['cmdCode'] == 'TOTAL':
                                    r['cmdCode'] = '0'

                                bulks = bulks + '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(r['pfCode'], r['yr'], r['period'], r['aggrLevel'], r['IsLeaf'], r['rgCode'], r['rtCode'], r['ptCode'], none_conv(r['cmdCode']), none_conv(r['qtCode']), none_conv(r['TradeQuantity']), none_conv(r['NetWeight']), none_conv(r['GrossWeight']), none_conv(r['TradeValue']), none_conv(r['CIFValue']), none_conv(r['FOBValue']))

                        if bulks:
                            with open(file_name + '.txt', 'w', encoding='utf-8') as f:
                                f.write(bulks)
                                execute = True

                            del bulks

                        del j

                    # CSV files to load
                    if file_name[-3:] == 'csv':
                        csv = pd.read_csv(file_name, low_memory=False)

                        # Does it have something?
                        if not csv.isnull().loc[0, 'Year']:
                            csv.drop(csv_cols, axis=1, inplace=True)

                            t = csv['Commodity Code'] == 'TOTAL'
                            csv.loc[t, 'Commodity Code'] = 0

                            csv.to_csv(s + '.txt', sep=',', header=False, index=False, encoding='utf-8')

                            execute = True

                    if execute:
                        try:
                            #res = con.execute("LOAD DATA INFILE '{}' IGNORE INTO TABLE comtrade FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' (Classification, Year, Period, AggrLevel, IsLeaf, TradeFlow, Reporter, Partner, Comm, QtyUnit, Qty, NetWeight, GrossWeight, TradeValue, CIFTradeValue, FOBTradeValue)".format(file_name + '.txt'))
                            res = con.execute("LOAD DATA INFILE '{}' IGNORE INTO TABLE comtrade FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'".format(file_name + '.txt'))

                            files_done.append(file_name)

                        except:
                            print('Error in file: ' + file_name + '.txt')

                        with open(log_file, 'w') as db_f:
                            db_f.write('\n'.join(files_done))

