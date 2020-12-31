import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util
import dateutil.parser as parser
class MongoDbClient(object):
    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')
        self.mongoDB = self.mongoClient["pet_data"]

    def getCurrent (self, sensorId):
        res = self.mongoDB["sensorData"].find({"sensorID":sensorId},{"_id": 0,"date":0}).sort([("date", -1)]).limit(1)
        results = list(res)
        if(len(results)>0):
            return (self._formJson("success",results))
        else:
            return (self._formJson("failed", None))

    def updateStatus (self, time,status):
        date = parser.parse(time)
        res = self.mongoDB["sensorData"].update_one({"date":date},{'$set': {"Status": status}})
        if res.acknowledged:
            reg_jsonres = {'Result': "Success", 'Message': "Updated"}
        else:
            reg_jsonres = {'Result': "Failed", 'Message': "Updated Failed"}
        print(reg_jsonres)
        return (json.dumps(reg_jsonres)).encode('utf8')

    def insertSensorData(self, inputdata):

        if (inputdata != ""):
            inputdata["date"] = datetime.strptime(inputdata["date"], '%Y-%m-%dT%H:%M:%S.%f')
            print(inputdata)
            res = self.mongoDB["sensorData"].insert_one(inputdata);
            print(res.acknowledged)
            if res.acknowledged:
                reg_jsonres = {'Result': "Success", 'Message': "Inserted"}
            else:
                reg_jsonres = {'Result': "Failed", 'Message': "Insertion Failed"}
            print(reg_jsonres)
        else:
            reg_jsonres = {'Result': "Success", 'Message': "Nothing to Insert"}
        return (json.dumps(reg_jsonres)).encode('utf8')

    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val}, default=json_util.default)).encode('utf8')

