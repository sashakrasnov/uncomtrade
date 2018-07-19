import os, json
import pandas as pd
from datetime import date
from sqlalchemy import create_engine

def none_conv(v):
    if v == None:
        return 0
    else:
        return v

# колонки в csv-файле, которые будем удалять, чтобы можно было сохранить все сразу
csv_cols = ('Period Desc.', 'Trade Flow', 'Reporter', 'Reporter ISO', 'Partner', 'Partner ISO', '2nd Partner Code', '2nd Partner', '2nd Partner ISO', 'Customs Proc. Code', 'Customs', 'Mode of Transport Code', 'Mode of Transport', 'Commodity', 'Qty Unit', 'Alt Qty Unit Code', 'Alt Qty Unit', 'Alt Qty', 'Flag')

# лог-файл, в нем будем хранить то, что успели обработать
log_file = 'db_update.log'

# период, по которому будем обновлять БД
last_year = date.today().year - 3
years = range(last_year, 2014, -1)

# открываем лог-файл того, что уже сделали
with open(log_file, 'r') as db_f:
    files_done = [x.strip() for x in list(db_f)] 

# устанавливаем соединение с БД
engine = create_engine('mysql://root:dvorak123@localhost:3306/agroexport')

with engine.connect() as con:
    for year in years:
        folder = './' + str(year) + '/'
        with os.scandir(folder) as dir:
            for fl in dir:
                file_name = folder + fl.name
                if not fl.name.startswith('.') and fl.is_file() and file_name not in files_done:
                    print(file_name)

                    # будем загружать файлы или нет. по-умолчанию -- нет
                    execute = False

                    # исходные данные в формате JSON
                    if file_name[-4:] == 'json':
                     
                        # загружаем данные в формате json
                        with open(file_name, 'r', encoding='utf-8') as f:
                            j = json.load(f)

                        bulks = ''

                        # есть ли вообще что-то в выборке
                        if j['validation']['count']['value'] > 0:

                            for r in j['dataset']:
                                # не является ли текущая запись суммой. исключаем ее
                                if r['cmdCode'] == 'TOTAL':
                                    r['cmdCode'] = '0'
                                    #continue

                                bulks = bulks + '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(r['pfCode'], r['yr'], r['period'], r['aggrLevel'], r['IsLeaf'], r['rgCode'], r['rtCode'], r['ptCode'], none_conv(r['cmdCode']), none_conv(r['qtCode']), none_conv(r['TradeQuantity']), none_conv(r['NetWeight']), none_conv(r['GrossWeight']), none_conv(r['TradeValue']), none_conv(r['CIFValue']), none_conv(r['FOBValue']))

                        if bulks:
                            with open(file_name + '.txt', 'w', encoding='utf-8') as f:
                                f.write(bulks)
                                execute = True

                            del bulks

                        # освобождаем json-структуру
                        del j

                    # исходные данные в формате CSV
                    if file_name[-3:] == 'csv':
                        csv = pd.read_csv(file_name, low_memory=False)

                        # проверяем выборку: пустая или не пустая
                        if not csv.isnull().loc[0, 'Year']:
                            for c in csv_cols:
                                del csv[c]

                            t = csv['Commodity Code'] == 'TOTAL'
                            csv.loc[t, 'Commodity Code'] = 0

                            csv.to_csv(s + '.txt', sep=',', header=False, index=False, encoding='utf-8')

                            execute = True

                    if execute:
                        try:
                            #res = con.execute("LOAD DATA INFILE '{}' IGNORE INTO TABLE comtrade FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' (Classification, Year, Period, AggrLevel, IsLeaf, TradeFlow, Reporter, Partner, Comm, QtyUnit, Qty, NetWeight, GrossWeight, TradeValue, CIFTradeValue, FOBTradeValue)".format(file_name + '.txt'))
                            res = con.execute("LOAD DATA INFILE '{}' IGNORE INTO TABLE comtrade FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'".format(file_name + '.txt'))

                            # если ошибки при добавлении не было
                            files_done.append(file_name)

                        except:
                            print('Error in file: ' + file_name + '.txt')
                        #    quit()


                        with open(log_file, 'w') as db_f:
                            db_f.write('\n'.join(files_done))

