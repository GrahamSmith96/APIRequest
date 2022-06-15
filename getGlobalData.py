import psycopg2
import requests
import json
import csv


#creating connection
conn = psycopg2.connect(database= 'GlobalData', user='postgres', password='password', host='127.0.0.1', port='5432')

#cursor instantiation, cursors act as the conduit of communication to the database
cursor = conn.cursor()

#fetch data from API
response = requests.get("https://kaidu-dev1.deeppixel.ai/globalData?endDate=2022-06-01T00%3A00%3A00-05%3A00&customer=e975dcfd-e348-499d-ab55-4d268e475db5")
data = response.json()
print(data)
#create table if it does not exist
# insert data fetched from API into the database

# executing an MYSQL function using the execute() method
cursor.execute(f'''
INSERT INTO
globaldata(uniquevisitors,
 averagedwelltime, 
 mostactivebeacon, 
 highestcontacts, 
 highestdate, 
 leastactivebeacon, 
 lowestcontacts, 
 lowestdate, 
 bluetooth, 
 wifi, 
 nonapple,
 snr,
 accum,
 overall)
VALUES
    ({data['uniqueVisitors']},
    {data['averageDwellTime']},
    {data['activity']['mostActiveBeacon']},
    {data['activity']['highest']['contacts']},
    {data['activity']['highest']['date']}
    {data['activity']['leastActiveBeacon']},
    {data['activity']['lowest']['contacts']},
    {data['activity']['lowest']['date']},
    {data['graph']['data'][0]['bluetooth']},
    {data['graph']['data'][0]['wifi']},
    {data['graph']['data'][0]['nonapple']},
    {data['graph']['data'][0]['snr']},
    {data['graph']['data'][0]['accum']},
    {data['graph']['data'][0]['overall']}
    )
''')

#commit the requests
conn.commit()


# close the connection 
conn.close()

