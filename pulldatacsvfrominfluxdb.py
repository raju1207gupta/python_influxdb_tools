import requests
import csv
import json

#prereqesite
##create a sample csvdatapyconfig.json in the same folder
# {
#   "IP_add": "locolhost",
#   "port": 8086,
#   "Database_Name" :'Influx_Database_Name',
#   "Tablename": 'Influx_Tablename',
#   "time_interval" :'time_interval_from_now'


# }


with open('csvdatapyconfig.json') as f:
  data = json.load(f)

IP_add = data['IP_add']
port = data['port']
Database_Name = data['Database_Name']
Tablename= data['Tablename']
time_interval = data['time_interval']


url = f"http://{IP_add}:{port}/query?pretty=true&db={Database_Name}&q=SELECT * FROM {Tablename} WHERE time >  now() -{time_interval}"

payload  = {}
headers = {
  'Accept': 'application/csv'
}

response = requests.request("GET", url, headers=headers, data = payload)
#print(response.text.encode('utf8'))
data = response.text.encode('utf8')
data = str(data)
data = data[2:-2]
#print(data)
datalist = data.split("\\n")
f = open(f'{Tablename}.csv', 'w' ,newline='')
w = csv.writer(f , delimiter=',')
w.writerows([x.split(',') for x in datalist])
f.close()
