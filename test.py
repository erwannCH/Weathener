import pandas as pd
import requests

headers = {
    'x-rapidapi-host': 'meteostat.p.rapidapi.com',
    'x-rapidapi-key': '49fb02cec0mshcda3381b4334509p148a16jsn66939543528d',
}

params = {
    'station': '07156',
    'start': '2022-01-01',
    'end': '2022-12-31',
}

response = requests.get('https://meteostat.p.rapidapi.com/stations/daily', params=params, headers=headers)
response = response.json()
df = pd.DataFrame(response["data"])

print(df)