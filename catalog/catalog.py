# -*- coding: utf-8 -*-
"""
Created on Sat Nov 09 18:24:47 2019

@author: cyril
"""
from datetime import datetime
import json
import cherrypy
import cherrypy_cors
from pymongo import MongoClient
import socket,struct,platform

class Catalog():

    def __init__(self):
        self.mongoClient = MongoClient('localhost:27017')
        self.mongoDB = self.mongoClient["Altron_base"]
        
    def get_ip_address(self,curOS):
        if curOS == "Linux":
            import fcntl
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.inet_ntoa(fcntl.ioctl(
                s.fileno(),
              0x8915,  # SIOCGIFADDR
             struct.pack('256s', "wlan0")
            )[20:24])
        elif curOS == "Windows":
            return(socket.gethostbyname(socket.gethostname()))
        else:
            return("127.0.0.1")

    def getLastUpdate(self, houseID):
        val = self.mongoDB["catalog"].find({"HouseID": houseID}, {"lastUpdate": 1, "_id": 0})
        return (self._formJson("success", val.next()))

    def getWebUsers(self, houseID):
        val = self.mongoDB["catalog"].find({"HouseID": houseID}, {"Web_Users": 1, "_id": 0})
        return (self._formJson("success", val.next()))

    def lastUpdate(self, houseID):
        updateTime = str(datetime.now()).rsplit(':', 1)[0]
        self.mongoDB["catalog"].update_one(
            {"HouseID": houseID},
            {"$set": {"lastUpdate": updateTime}}
        )
    
    def getSensorKeys(self,houseID,sensorType):
        val = self.mongoDB["catalog"].find({"HouseID": houseID},
                          {"SensorKeys.{}".format(sensorType): 1, "_id": 0})
        return (self._formJson("success", val.next()))
    
    def getTelegramUsers(self, houseID):
        val = self.mongoDB["catalog"].find({"HouseID": houseID}, {"Tel_Users": 1, "_id": 0})
        return (self._formJson("success", val.next()))

    def getInfo(self, deviceID):
        val = self.mongoDB["catalog"].find \
            ({"Devices.{}".format(deviceID): {"$exists": True}}
             , {"_id": 0})
        
        res = val.next()
        res["Devices"][deviceID]["DC"] = (res["Devices"][deviceID]["DC"].format(self.get_ip_address(platform.system())))
        res["Devices"][deviceID]["SC"] = (res["Devices"][deviceID]["SC"].format(self.get_ip_address(platform.system())))
        return (self._formJson("success", res))

    def geturlInfo(self, houseID, deviceID):
        val = self.mongoDB["catalog"].find \
            ({"HouseID": houseID }
             , {"_id": 0,"lastUpdate": 1,"Devices.{}.DC".format(deviceID):1
                ,"Devices.{}.SC".format(deviceID):1,"Devices.{}.Sensors".format(deviceID):1})
        res = val.next()
        res["Devices"][deviceID]["DC"] = (res["Devices"][deviceID]["DC"].format(self.get_ip_address(platform.system())))
        res["Devices"][deviceID]["SC"] = (res["Devices"][deviceID]["SC"].format(self.get_ip_address(platform.system())))
        return (self._formJson("success", res))

    def getserverurl(self, houseID):
        val = self.mongoDB["catalog"].find \
            ({"HouseID": houseID }
             , {"_id": 0,"serverURL":1})
        return (self._formJson("success", val.next()))

    def getsensorInfo(self, houseID, deviceID):
        val = self.mongoDB["catalog"].find \
            ({"HouseID": houseID }
             , {"_id": 0,"Devices.{}.Sensors".format(deviceID):1})
        return (self._formJson("success", val.next()))

    def addWebUser(self, houseID, users):
        for i in users:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$push": {"Web_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "fail to add user"))

    def addTelegramUser(self, houseID, users):
        for i in users:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$push": {"Tel_Users": int(i.encode())}}
            )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "fail to add user"))
    

    def addDevices(self, houseID, data):

        addType = data["type"]
        devID = data["deviceID"]
        val = data["values"]
        print(val)
        if addType == "All" and val["DC"] and val["SC"] and val["Sensors"]:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$set": {"Devices.{}".format(devID): val}}
            )
        elif addType == "Sensors" and val:
            for i in val:
                res = self.mongoDB["catalog"].update_one(
                    {"HouseID": houseID},
                    {"$push": {"Devices.{}.Sensors".format(devID): i}}
                )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "fail to add user"))

    def removeTelegramUser(self, houseID, users):
        for i in users:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$pull": {"Tel_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "Failed to Remove user"))

    def removeWebUser(self, houseID, users):
        for i in users:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$pull": {"Web_Users": i}}
            )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "Failed to Remove user"))

    def removeDevice(self, houseID, data):

        devType = data["type"]
        devID = data["deviceID"]
        if devType == "All" and devID:
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$unset": {
                    "Devices.{}".format(devID): 1}}
            )
        elif devType == "Sensors" and data["values"] and devID:
            for i in data["values"]:
                res = self.mongoDB["catalog"]. \
                    update_one(
                    {"HouseID": houseID},
                    {"$pull": {"Devices.{}.Sensors".format(devID): i}}
                )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "Failed to Remove user"))

    def updateDeviceURLs(self, houseID, data):
        devID = data["deviceID"]
        val = data["values"]
        for i in val.keys():
            res = self.mongoDB["catalog"].update_one(
                {"HouseID": houseID},
                {"$set": {"Devices.{}.{}".format(devID, i): val[i]}}
            )
        if res.acknowledged:
            self.lastUpdate(houseID)
            return (self._formJson("success", "Updated"))
        else:
            return (self._formJson("failed", "Failed to update URLS"))

    def _formJson(self, status, val):
        return (json.dumps({'Result': status, 'Output': val}))


class CatalogWebService(object):
    exposed = True

    def GET(self, *uri, **params):

        cat = Catalog()
        res = None
        if str(uri[0]) == "getinfo" and len(uri) == 2:
            res = cat.getInfo(uri[1])
        
        elif str(uri[0]) == "geturlinfo" and len(uri) == 3:
            res = cat.geturlInfo(uri[1],uri[2])
        
        elif str(uri[0]) == "getserverurl" and len(uri) == 2:
            res = cat.getserverurl(uri[1])
        
        elif str(uri[0]) == "getsensorInfo" and len(uri) == 3:
            res = cat.getsensorInfo(uri[1],uri[2])
            
        elif str(uri[0]) == "getwebusers" and len(uri) == 2:
            res = cat.getWebUsers(uri[1])

        elif str(uri[0]) == "gettelusers" and len(uri) == 2:
            res = cat.getTelegramUsers(uri[1])

        elif str(uri[0]) == "getlastupdate" and len(uri) == 2:
            res = cat.getLastUpdate(uri[1])
        
        elif str(uri[0]) == "getkeys" and len(uri) == 3:
            res = cat.getSensorKeys(uri[1],uri[2])

        else:
            return cat._formJson("Failed", "Wrong request")

        return (res)

    def POST(self, *uri, **params):
        cat = Catalog()
        res = None
        inputtext = json.loads(cherrypy.request.body.read())
        method = inputtext["call"]
        houseID = inputtext["HouseID"]

        if method == "addwebusers":
            res = cat.addWebUser(houseID, inputtext["Users"])

        elif method == "addtelusers":
            res = cat.addTelegramUser(houseID, inputtext["Users"])

        elif method == "adddevices":
            res = cat.addDevices(houseID, inputtext["data"])

        elif method == "remwebusers":
            res = cat.removeWebUser(houseID, inputtext["Users"])

        elif method == "remtelusers":
            res = cat.removeTelegramUser(houseID, inputtext["Users"])

        elif method == "remdevices":
            res = cat.removeDevice(houseID, inputtext["data"])

        elif method == "updatedevices":
            res = cat.updateDeviceURLs(houseID, inputtext["data"])

        else:
            return cat._formJson("Failed", "Wrong request")

        return (res)

    def PUT(self, *uri, **params):
        pass

    def DELETE(self):
        pass
    
def CORS():
    cherrypy.response.headers["Access-Control-Allow-Origin"] = "*"
    

if __name__ == '__main__':
    cherrypy_cors.install()
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
			'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.CORS.on': True,
            'cors.expose.on': True,
            'tools.response_headers.headers':
            [('Content-Type', 'application/json'),]
        }
    }
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8181,
    })
    cherrypy.tools.CORS = cherrypy.Tool('before_handler', CORS)
    cherrypy.tree.mount(CatalogWebService(), '/catalog', conf)
    cherrypy.engine.start()
    cherrypy.engine.block()   