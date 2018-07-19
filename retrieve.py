import pandas as pd, os
from urllib.request import urlretrieve
from time import sleep
from datetime import date
import requests
import certifi
import urllib3

#reporter_url = 'https://comtrade.un.org/data/cache/reporterAreas.json'
reporter_url = 'reporterAreas.json'
#partner_url = 'https://comtrade.un.org/data/cache/partnerAreas.json'
partner_url = 'partnerAreas.json'

# период, за который будем вытягивать данные
last_year = date.today().year - 1
years = range(last_year, 2011, -1)

# список стран, для которых будем делать выгрузки. если список пустой, то для всех
valid_reporters = ()
valid_partners  = ()

# в каком виде будем забирать: json или csv. первый вариант более полный
data_type = 'json'

# получаем список кодов регионов, они содержатся в колонке 'results'
reporters = pd.read_json(reporter_url)
partners  = pd.read_json(partner_url)

for year in years:
    dir = './' + str(year)

    print(year)

    try:
        os.mkdir(dir)
    except FileExistsError:
        pass

    for reporter in reporters['results']:
        # исключаем "все страны" и проверяем есть ли что-то в списке стран
        if reporter['id'] == 'all' or (valid_reporters and int(reporter['id']) not in valid_reporters):
            continue

        for partner in partners['results']:
            fl = dir + '/' + str(reporter['id']) + '-' + str(partner['id']) + '.'

            # Если регион = все, или два региона одинаковы или файл уже есть, переходим к следующему
            if partner['id'] == 'all' or reporter['id'] == partner['id'] or (valid_partners and int(partner['id']) not in valid_partners):
                continue

            print('Fetching data for', reporter['text'], '-', partner['text'])

            #если уже вытягивали ранее, в независимости от формата
            if os.path.isfile(fl + 'csv') or os.path.isfile(fl + 'json'):
                print(fl + '[csv|json]', '-> File already exists\n')
                continue

            # вычисляем адрес нужной выгрузки
            #url = 'https://comtrade.un.org/api/get?max=100000&type=C&freq=A&px=HS&ps=' + str(year) + '&r=' + reporter['id'] + '&p=' + partner['id'] + '&rg=all&cc=ALL&fmt=' + data_type
            url = 'https://comtrade.un.org/api/get?max=100000&type=C&freq=A&px=HS&ps={}&r={}&p={}&rg=all&cc=ALL&fmt={}'.format(year, reporter['id'], partner['id'], data_type)

            print(fl + data_type)

            # вариант 1
            http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

            try:
                #urlretrieve(url, fl + data_type)

                r = http.request('GET', url)
            
                with open(fl + data_type, 'wb') as f:
                    f.write(r.data)
            
                # вариант 2
                #with open(fl + data_type, 'wb') as f:
                #    resp = requests.get(url, verify=False)
                #    f.write(resp.content)

                    print('+ Done\n')

            except:
                print('- Error retreiving\n')

            # ждем 40 секунд в соответствии с ограничением API
            sleep(40)
