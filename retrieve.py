'''
UN Comtrade data retriever
'''


import pandas as pd, os
from urllib.request import urlretrieve
from time import sleep
from datetime import date
import requests
import certifi
import urllib3


# List of countries-reporters: id/country
#reporter_url = 'https://comtrade.un.org/data/cache/reporterAreas.json'
reporter_url = 'reporterAreas.json'

# List of countries-partners: id/country
#partner_url = 'https://comtrade.un.org/data/cache/partnerAreas.json'
partner_url = 'partnerAreas.json'

# Data interval
last_year = date.today().year - 1
years = range(last_year, 2011, -1)

# List of countries to be load. Empty list for all countries
valid_reporters = ()
valid_partners  = ()

# Default data type. JSON data from UN Comtrade is more complete than CSV
data_type = 'json'

# List of regions
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
        # excluding unnecessary countries
        if reporter['id'] == 'all' or (valid_reporters and int(reporter['id']) not in valid_reporters):
            continue

        for partner in partners['results']:
            fl = dir + '/' + str(reporter['id']) + '-' + str(partner['id']) + '.'

            # Skip if all partners, same regions or file already retrieved
            if partner['id'] == 'all' or reporter['id'] == partner['id'] or (valid_partners and int(partner['id']) not in valid_partners):
                continue

            print('Fetching data for', reporter['text'], '-', partner['text'])

            # Data already retrieved and saved previously
            if os.path.isfile(fl + 'csv') or os.path.isfile(fl + 'json'):
                print(fl + '[csv|json]', '-> File already exists\n')

                continue

            # URL of UN Comtrade data to be retrieved
            #url = 'https://comtrade.un.org/api/get?max=100000&type=C&freq=A&px=HS&ps=' + str(year) + '&r=' + reporter['id'] + '&p=' + partner['id'] + '&rg=all&cc=ALL&fmt=' + data_type
            url = 'https://comtrade.un.org/api/get?max=100000&type=C&freq=A&px=HS&ps={}&r={}&p={}&rg=all&cc=ALL&fmt={}'.format(year, reporter['id'], partner['id'], data_type)

            print(fl + data_type)

            # Var. 1
            http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

            try:
                #urlretrieve(url, fl + data_type)

                r = http.request('GET', url)
            
                with open(fl + data_type, 'wb') as f:
                    f.write(r.data)
            
                # Var. 2
                #with open(fl + data_type, 'wb') as f:
                #    resp = requests.get(url, verify=False)
                #    f.write(resp.content)

                    print('+ Done\n')

            except:
                print('- Error retreiving\n')

            # Wait for 40 sec to skip UN Comtrade API restriction
            sleep(40)
