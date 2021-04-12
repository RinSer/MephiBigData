import os, requests

'''
Скрипт, собирающий все возможные пары
Аэропорт -> Страна
с API сервиса aviationstack.com
'''

params = {
  'access_key': os.environ['AVIA_TOKEN']
}

api_result = requests.get('http://api.aviationstack.com/v1/airports', params)
api_response = api_result.json()

pagination = api_response['pagination']
total = int(pagination['total'])
limit = int(pagination['limit'])
max_offset = total - (total % limit)

with open('data/airports2countries.txt', 'w', encoding='utf-16') as file:
    for i in range(limit, max_offset + (2 * limit), limit):
        for result in api_response['data']:
            airport = result['airport_name'].strip()
            country = result['country_name'].strip() if result['country_name'] else 'Unknown'
            file.write(airport + '\t' + country + '\n')
        if i < max_offset + limit:
            params['offset'] = i
            api_result = requests.get('http://api.aviationstack.com/v1/airports', params)
            api_response = api_result.json()