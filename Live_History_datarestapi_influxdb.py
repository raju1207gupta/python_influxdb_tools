from flask import Flask, jsonify, request, Response
import json
import threading 
from influxdb import InfluxDBClient
app = Flask(__name__)
data={}
import paho.mqtt.client as mqtt


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("DAQSERVICE1_MODBUS_DAQ_LIVE")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    topicname=msg.topic
    #data=str(msg.payload)
    global data
    ds=(msg.payload).decode('utf-8')
    data = json.loads(ds)
    #print(data)

def mqttsub():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    client.loop_forever()

def restcall():
    app.run(port=5000)


@app.route('/LiveData',methods=['POST'])
def Get_LiveDatatest():
    response = Response(json.dumps(data), status=201, mimetype='application/json')
    return response

@app.route('/HistoryData' , methods=['POST'])
def Get_HistoryData():
    request_data = request.get_json() 
    try:
        dbname = request_data["dbname"]
        measurementname = request_data["tablename"]
        client1.switch_database(dbname)
        if("timeinterval" in request_data):
            if("asset" in request_data):
                if("tag" in request_data ):
                    asset = request_data["asset"]
                    tag = request_data["tag"]
                    timeinterval = request_data["timeinterval"]
                    if("queryitems" in request_data):
                        queryitems = request_data["queryitems"]
                        Historydata = client1.query(f"select {queryitems} from {measurementname} WHERE \"tag\" ='{tag}' AND asset='{asset}' AND time > now()- {timeinterval}")
                        Historydata = list(Historydata.get_points(measurement=measurementname))
                    else:
                        Historydata = client1.query(f"select value,unit,messageid,assetcode,timezone,component from {measurementname} WHERE \"tag\" ='{tag}' AND asset='{asset}' AND time > now()- {timeinterval}")
                        Historydata = list(Historydata.get_points(measurement=measurementname))
                        
                else:
                    asset = request_data["asset"]
                    timeinterval = request_data["timeinterval"]
                    Historydata = client1.query(f"select * from {measurementname} WHERE asset='{asset}' AND time > now()- {timeinterval}")
                    Historydata = list(Historydata.get_points(measurement=measurementname))

            else:
                timeinterval = request_data["timeinterval"]
                Historydata = client1.query(f'select * from {measurementname} WHERE time > now()- {timeinterval}')
                Historydata = list(Historydata.get_points(measurement=measurementname))
        
        elif("starttime" in request_data and "endtime" in request_data):
            if("asset" in request_data):
                if("tag" in request_data):
                    asset = request_data["asset"]
                    tag = request_data["tag"]
                    starttime = request_data["starttime"]
                    endtime = request_data["endtime"]
                    if("queryitems" in request_data):
                        queryitems = request_data["queryitems"]
                        Historydata = client1.query(f"select {queryitems} from {measurementname} WHERE \"tag\" ='{tag}' AND asset='{asset}' AND time >= '{starttime}' and time < '{endtime}'")
                        Historydata = list(Historydata.get_points(measurement=measurementname))
                    else:
                        Historydata = client1.query(f"select value,unit,messageid,assetcode,timezone,component from {measurementname} WHERE \"tag\" ='{tag}' AND asset='{asset}' AND time >= '{starttime}' and time < '{endtime}'")
                        Historydata = list(Historydata.get_points(measurement=measurementname))
                    
                else:
                    asset = request_data["asset"]
                    starttime = request_data["starttime"]
                    endtime = request_data["endtime"]
                    Historydata = client1.query(f"select * from {measurementname} WHERE asset='{asset}' AND time >= '{starttime}' and time < '{endtime}'")
                    Historydata = list(Historydata.get_points(measurement=measurementname))
            
        
            else:
                starttime = request_data["starttime"]
                endtime = request_data["endtime"]
                Historydata = client1.query(f"select * from {measurementname} WHERE time >= '{starttime}' and time < '{endtime}'")
                Historydata = list(Historydata.get_points(measurement=measurementname))
        
        
        #print(Historydata)
        #Historydata = json.dumps(Historydata)
        response = Response(json.dumps(Historydata), status=201, mimetype='application/json')
    except:
        invalidErrorMsg = {
        "error": "Invalid request",
        "helpString": "Insufficent Data or wrong data entered for query in the body"
        }
        response = Response(json.dumps(invalidErrorMsg), status=400, mimetype='application/json')

        # http://localhost:8086/query?pretty=true&db=ITCCHILLER&q=SELECT * FROM ITCCHILLER01 WHERE  time >  now() -3m
    return (response)

    





if __name__ == "__main__":
    client1 = InfluxDBClient('localhost', 8086)
    t1 = threading.Thread(target=mqttsub) 
    t2 = threading.Thread(target=restcall) 
    # starting thread 1 
    t1.start() 
    # starting thread 2 
    t2.start() 
  
    # wait until thread 1 is completely executed 
    t1.join() 
    # wait until thread 2 is completely executed 
    t2.join() 
    
    
   