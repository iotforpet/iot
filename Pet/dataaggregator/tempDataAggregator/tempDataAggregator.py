import json
import time
from datetime import datetime, timedelta
import numpy as np
import paho.mqtt.client as PahoMQTT
import requests


class MyMQTT:
    def __init__(self, broker, port, notifier, houseID):
        self.broker = broker
        self.port = port
        self.notifier = notifier
        self.houseID = houseID
        self._paho_mqtt = PahoMQTT.Client("temperature_Aggregator", False)
        self._paho_mqtt.on_connect = self.myOnConnect
        self._paho_mqtt.on_message = self.myOnMessageReceived

    def myOnConnect(self, paho_mqtt, userdata, flags, rc):
        pass
        # print ("Connected to message broker with result code: " + str(rc))

    def myOnMessageReceived(self, paho_mqtt, userdata, msg):
        self.notifier.notify(msg.topic, msg.payload)

    def myPublish(self, plug_id, name, data):
        pw_topic = 'house/' + str(self.houseID) + '/temperature/' + plug_id + '/aggregated_temp'
        js = {"Appliance_Name": name, "value": data}
        print(js)
        self._paho_mqtt.publish(pw_topic, json.dumps(js), 2)

    def mySubscribe(self, topicRawTemp, qos=2):
        self._paho_mqtt.subscribe(topicRawTemp, qos)

    def myUnSubscribe(self, topic):
        self._paho_mqtt.unsubscribe(topic)

    def start(self):
        print(self.port)
        self._paho_mqtt.connect(self.broker, int(self.port))
        self._paho_mqtt.loop_start()

    def stop(self):
        self._paho_mqtt.disconnect()
        self._paho_mqtt.loop_stop()


class pwDataAggregator:

    def __init__(self):
        self.systemFile = open("baseConfig.json")
        self.systemInfo = json.loads(self.systemFile.read())
        self.systemFile.close()
        self.dc_searchby = 'sensors'
        self.device_search = 'T-Sensors'
        self.mqttStatus = 0
        self.restartSystem = 0
        self.tempSensorstodelete = {}
        self.tempSensorstoadd = []
        self.initSystem()

    def initSystem(self):
        self.deviceID = self.systemInfo["deviceID"]
        self.catalogURL = self.systemInfo["catalogURL"]
        self.getInfo = json.loads(requests.get(self.catalogURL + "/getinfo/" + self.deviceID).text)
        if self.getInfo["Result"] == "success":
            self.info = self.getInfo["Output"]
            self.houseID = self.info["HouseID"].encode()
            self.dcUrl = self.info["Devices"][self.deviceID]["DC"].encode()
            self.scUrl = self.info["Devices"][self.deviceID]["SC"].encode()
        else:
            print("System Initialisation Failed due to Resource Catalog Issues")
            time.sleep(60)
            self.initSystem()

        self.getUpdate = json.loads(requests.get(self.catalogURL + "/getlastupdate/" + self.houseID).text)
        if self.getUpdate["Result"] == "success":
            self.catalog_lastUpdate = self.getUpdate["Output"]["lastUpdate"]

        self.deviceConfigurations()
        self.serviceConfigurations()
        self.startSystem()
        self.checkCatalogRegister()
        self.unsubscribeToTopics()
        self.subscribeToTopics()
        if self.restartSystem:
            print("Restarted")
            self.restartSystem = 0
            self.aggregate_Data()

    def deviceConfigurations(self):

        # Device Details
        self.device_details = json.loads(requests.get(self.dcUrl
                                                      + self.houseID + "/"
                                                      + self.deviceID + "/"
                                                      + self.dc_searchby + "/"
                                                      + self.device_search).text)
        if self.device_details["Result"] == "success":
            if self.restartSystem:
                self.new_tempSensors = self.device_details["Output"]["installed{}".format(self.device_search)]
            else:
                self.tempSensors = self.device_details["Output"]["installed{}".format(self.device_search)]

        else:
            print("Couldnt recover device details")
            time.sleep(60)
            self.deviceConfigurations()

        getDevUpdate = json.loads(requests.get(self.dcUrl
                                               + self.houseID + "/"
                                               + self.deviceID + "/getlastupdate").text)
        self.device_lastUpdate = getDevUpdate["Output"]

    def serviceConfigurations(self):
        # Service Details
        servReq = {
            "call": "getService",
            "houseID": self.houseID,
            "deviceID": self.deviceID,
            "data": ["MQTT", "last_update", "DataCollection", self.device_search]
        }
        serviceResp = json.loads(requests.post(self.scUrl, json.dumps(servReq)).text)

        if serviceResp["Result"] == "success":
            self.mqtt_broker = serviceResp["Output"]["MQTT"]["mqtt_broker"]
            self.mqtt_port = serviceResp["Output"]["MQTT"]["mqtt_port"]
            self.service_lastUpdate = serviceResp["Output"]["last_update"]
            self.frequency = serviceResp["Output"][self.device_search]["Frequency"]
            self.insertDataAPI = serviceResp["Output"]["DataCollection"]
        else:
            print("couldnt recover service details. Trying again")
            time.sleep(60)
            self.serviceConfigurations()

    def startSystem(self):
        if self.restartSystem:
            print("restarting")
            tempIDs = [i["ID"] for i in self.new_tempSensors]
            if self.temp_readings:
                delta = list(set(tempIDs).symmetric_difference(set(self.temp_readings.keys())))
                for j in delta:
                    if j in self.temp_readings.keys():
                        print("removing temp sensor:{}".format(j))
                        self.tempSensorstodelete[j] = [i["Name"] for idx, i in enumerate(self.tempSensors) if i["ID"] == j][0]
                        [self.tempSensors.pop(idx) for idx, i in enumerate(self.tempSensors) if i["ID"] == j]
                    else:
                        print("adding temp sensor:{}".format(j))
                        self.temp_readings[j] = []
                        self.agg_temp_readings[j] = []
                        self.tempSensorstoadd.append(j)
                        [self.tempSensors.append(i) for i in self.new_tempSensors if i["ID"] == j]
            else:
                self.temp_readings = {i["ID"]: [] for i in self.new_tempSensors}
                self.agg_temp_readings = {i["ID"]: [] for i in self.new_tempSensors}
            self.clearAggregatedReadings()
        else:
            self.temp_readings = {i["ID"]: [] for i in self.tempSensors}
            self.agg_temp_readings = {i["ID"]: [] for i in self.tempSensors}

        self.myMqtt = MyMQTT(self.mqtt_broker, self.mqtt_port, self, self.houseID)
        self.myMqtt.start()\
        # else:
        #     self.myMqtt.stop()
        #     time.sleep(0.1)
        #     self.myMqtt.start()

        # tl.start(block=False)

    def checkCatalogRegister(self):
        # checking device registration in catalog
        for i in self.tempSensors:
            if i['ID'] not in self.info["Devices"][self.deviceID]["Sensors"]:
                print("device not registered in Resource Catalog. Registering now..")
                reg_device = {
                    "call": "adddevices",
                    "HouseID": self.houseID,
                    "data": {"type": "Sensors", "deviceID": self.deviceID, "values": [i["ID"]]}
                }
                requests.post(self.catalogURL, reg_device)

    def subscribeToTopics(self):
        for i in self.tempSensorstoadd:
            self.topicRawPwr = 'house/' + str(self.houseID) + '/temperature/' + i + '/rwTmp'
            self.myMqtt.mySubscribe(self.topicRawPwr)
        for i in self.tempSensors:
            self.topicRawPwr = 'house/' + str(self.houseID) + '/temperature/' + i['ID'] + '/rwTmp'
            self.myMqtt.mySubscribe(self.topicRawPwr)

    def unsubscribeToTopics(self):
        for i in self.tempSensorstodelete.keys():
            self.topicRawPwr = 'house/' + str(self.houseID) + '/temperature/' + i + '/rwTmp'
            self.myMqtt.myUnSubscribe(self.topicRawPwr)

    def clearAggregatedReadings(self):
        for idx, tempSen in enumerate(self.tempSensorstodelete.keys()):
            print("Clearing Readings")
            self.agg_temp_readings[tempSen].append(
                np.mean(self.temp_readings[tempSen][:]))
            self.temp_readings.pop(tempSen, None)
            oneHourAvg = np.mean(self.agg_temp_readings[tempSen])
            dt = datetime.now() - timedelta(hours=1)
            sensorData = json.dumps({
                "call": "insert",
                "data": {
                    "date": dt.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                    "sensorType": self.device_search,
                    "sensorID": tempSen,
                    "sensorName": self.tempSensorstodelete[tempSen],
                    "sensorData": oneHourAvg,
                    "avgtime": len(self.agg_temp_readings[tempSen]) * self.frequency,
                    "consumptionCost": ''
                }
            })
            requests.post(self.insertDataAPI["DataInsertAPI"], sensorData)
            self.agg_temp_readings.pop(tempSen, None)
            self.tempSensorstodelete.pop(tempSen, None)


    def notify(self, topic, msg):
        try:
            tempSensorID = topic.split("/")[3]
            self.temp_readings[tempSensorID].append(float((json.loads(msg)['value'])))
        except KeyError:
            print("Yet to add the sensor")

    def checkConfigUpdates(self):
        getCatUpdate = json.loads(requests.get(self.catalogURL + "/getlastupdate/" + self.houseID).text)
        catUpdate = getCatUpdate["Output"]["lastUpdate"]
        getDevUpdate = json.loads(requests.get(self.dcUrl
                                               + self.houseID + "/"
                                               + self.deviceID + "/getlastupdate").text)
        devUpdate = getDevUpdate["Output"]
        getserUpdate = json.loads(requests.get(self.scUrl
                                               + self.houseID + "/"
                                               + self.deviceID + "/last_update").text)
        serUpdate = getserUpdate["Output"]

        if (catUpdate != self.catalog_lastUpdate) \
                or (devUpdate != self.device_lastUpdate) \
                or (serUpdate != self.service_lastUpdate):
            self.restartSystem = 1
            self.initSystem()

    def aggregate_Data(self):
        while not self.restartSystem:
            self.checkConfigUpdates()
            for idx, tempSen in enumerate(self.tempSensors):
                if len(self.temp_readings[tempSen["ID"]]) >= self.frequency:
                    print(self.agg_temp_readings[tempSen["ID"]])
                    self.agg_temp_readings[tempSen["ID"]].append(
                        np.mean(self.temp_readings[tempSen["ID"]][:self.frequency])
                    )
                    self.myMqtt.myPublish(tempSen["ID"], tempSen['Name'],
                                          np.mean(self.temp_readings[tempSen["ID"]][:self.frequency]))
                    del self.temp_readings[tempSen["ID"]][:self.frequency]
                    if len(self.agg_temp_readings[tempSen["ID"]]) == (60 / self.frequency):
                        oneHourAvg = np.mean(self.agg_temp_readings[tempSen["ID"]])
                        dt = datetime.now() - timedelta(hours=1)
                        sensorData = json.dumps({
                            "call": "insert",
                            "data": {
                                "date": dt.strftime('%Y-%m-%dT%H:%M:%S.%f'),
                                "sensorType": self.device_search,
                                "sensorID": tempSen["ID"],
                                "sensorName": tempSen["Name"],
                                "sensorData": oneHourAvg,
                                "avgtime": 60,
                                "consumptionCost": ''
                            }
                        })
                        requests.post(self.insertDataAPI["DataInsertAPI"], sensorData)
                        del self.agg_temp_readings[tempSen["ID"]][:]
            time.sleep(120)
            # time.sleep(5)


if __name__ == "__main__":
    aggregate = pwDataAggregator()
    aggregate.aggregate_Data()
#
