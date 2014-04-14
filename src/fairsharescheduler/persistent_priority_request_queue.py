from Queue import PriorityQueue
from nova.openstack.common import log as logging
import MySQLdb
import json
from DBUtils.PooledDB import PooledDB
import mysql.connector
import threading

__author__ = "Eric Frizziero, Lisa Zangrando"
__email__ = "eric.frizziero[AT]pd.infn.it, lisa.zangrando[AT]pd.infn.it"

LOG = logging.getLogger(__name__)

class PersistentPriorityRequestQueue(PriorityQueue):
    def __init__(self, mysqlHost, mysqlUser, mysqlPasswd, mysqlDatabase, poolSize=10):
        PriorityQueue.__init__(self)
        self.isDestroying = False
        self.condition = threading.Condition()
        self.poolSize = poolSize
        
        if self.poolSize < 1:
            self.poolSize = 10
            
        self.pool = PooledDB(mysql.connector, self.poolSize, database=mysqlDatabase, user=mysqlUser, passwd=mysqlPasswd, host=mysqlHost)
        self.__buildFromDB()

    def getRequestList(self):
        listQueue=[]
        while not self.empty():
            requestT = self.get()
            listQueue.append(requestT)
        return listQueue


    def putRequestList(self, listQueue):
        for requestT in listQueue:
            self.put(requestT)


    def putRequest(self, request, priority):
        with self.condition:
            LOG.info("putRequest condition acquired")
            idRecord = self.__putDB(request)
            requestPQ = {"priority": priority, 
                         "userId": request["userId"], 
                         "projectId": request["projectId"], 
                         "timestamp": request["timestamp"], 
                         "retryCount" : 0,
                         "idRecord": idRecord} 
            PriorityQueue.put(self, (priority, requestPQ))
            self.condition.notifyAll()
        LOG.info("putRequest condition released")


    def getRequest(self):
        with self.condition:
            LOG.info("getRequest condition acquired")
            requestPQ = None
            while ((requestPQ == None) & (not self.isDestroying)):
                if PriorityQueue.qsize(self) > 0:
                    _, requestPQ = PriorityQueue.get(self)
                else:
                    self.condition.wait()
            LOG.info("isDestroying=%s" % self.isDestroying)
            requestDB = None
            if (not self.isDestroying):
                LOG.info("idRecord %s" % (requestPQ["idRecord"]))
                idRecord = requestPQ["idRecord"]
                requestDB = self.__getDB(idRecord)
                requestDB.update(requestPQ)
            self.condition.notifyAll()
        request = requestDB
        LOG.info("getPQ condition released")
        return request
    
    
    def priorityUpdater(self, updater):
        with self.condition:
            LOG.info("priorityUpdater condition acquired")
            listQueue = self.getRequestList()
            listQueueNew = []
            for itemT in listQueue:
                requestPQ = itemT[1]
                newPriority = updater(itemT[1]["userId"],itemT[1]["projectId"], itemT[1]["timestamp"])
                requestPQ["priority"] = newPriority
                listQueueNew.append((newPriority, requestPQ))
            self.putRequestList(listQueueNew)
            self.condition.notifyAll()
        LOG.info("priorityUpdater condition released")
        LOG.info("Queue priorities updated!")
    
        
    def __putDB(self, itemValue):
        idRecord = -1
        try:
            dbConnection=self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute("insert into priority_queue (message_context, request_spec, admin_password, injected_files, requested_networks, is_first_time, filter_properties, legacy_bdm_in_spec) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", [json.dumps(itemValue["message_context"]), json.dumps(itemValue["request_spec"]), itemValue["admin_password"], json.dumps(itemValue["injected_files"]), json.dumps(itemValue["requested_networks"]), itemValue["is_first_time"], json.dumps(itemValue["filter_properties"]), itemValue["legacy_bdm_in_spec"]])
            idRecord=cursor.lastrowid
            dbConnection.commit()
        except MySQLdb.Error, e:
            if dbConnection:
                dbConnection.rollback()
                LOG.info("Error %d: %s" % (e.args[0],e.args[1]))
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  
        return idRecord
    
    
    def __getDB(self, idRecord):
        try:
            dbConnection=self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute("""select message_context, request_spec, admin_password, injected_files, requested_networks, is_first_time, filter_properties, legacy_bdm_in_spec from priority_queue WHERE id=%s""", [idRecord])
            item = cursor.fetchone()
            requestDB = {"message_context" : json.loads(item[0]),
                         "request_spec": json.loads(item[1]), 
                         "admin_password": item[2],
                         "injected_files": json.loads(item[3]),
                         "requested_networks": json.loads(item[4]),
                         "is_first_time": [True, False][item[5]], 
                         "filter_properties": json.loads(item[6]), 
                         "legacy_bdm_in_spec": [True, False][item[7]]}
            #cursor.execute("""delete from priority_queue WHERE id=%s""", [idRecord])
            #dbConnection.commit() 
        except MySQLdb.Error, e:
            #if dbConnection:
                #dbConnection.rollback()
            LOG.info("Error %d: %s" % (e.args[0],e.args[1]))
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  
        return requestDB

    def deleteRequestDB(self, idRecord):
        try:
            dbConnection=self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute("""delete from priority_queue WHERE id=%s""", [idRecord])
            dbConnection.commit() 
        except MySQLdb.Error, e:
            if dbConnection:
                dbConnection.rollback()
                LOG.info("Error %d: %s" % (e.args[0],e.args[1]))
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  
   
    def reInsertRequest(self, request):
        requestPQ = {"priority": request["priority"], 
                     "userId": request["userId"], 
                     "projectId": request["projectId"], 
                     "timestamp": request["timestamp"],
                     "retryCount": request["retryCount"], 
                     "idRecord": request["idRecord"]} 
        LOG.info("priority %s userId %s projectId %s timestamp %s retryCount %s idRecord %s" % (request["priority"], request["userId"],request["projectId"], request["timestamp"],request["retryCount"], request["idRecord"]))
        PriorityQueue.put(self, (request["priority"], requestPQ))


 
    def __buildFromDB(self):
        try:
            dbConnection=self.pool.connection()
            cursor = dbConnection.cursor()
            cursor.execute("""select request_spec, id from priority_queue""")
            moreDBElement = True
            while (moreDBElement):
                item = cursor.fetchone()
                if item:
                    request_spec = json.loads(item[0])
                    idRecord = item[1]
                    instance_properties = request_spec.get('instance_properties')
                    userId = instance_properties.get('user_id')
                    projectId = instance_properties.get('project_id')
                    timestamp = instance_properties.get('created_at')
    
                    requestPQ = {"priority": 0, 
                                 "userId": userId, 
                                 "projectId": projectId, 
                                 "timestamp": timestamp, 
                                 "retryCount": 0,
                                 "idRecord": idRecord} 
                    PriorityQueue.put(self, (0, requestPQ))
                else: 
                    moreDBElement = False
                
        except MySQLdb.Error, e:
            if dbConnection:
                dbConnection.rollback()
                LOG.info("Error %d: %s" % (e.args[0],e.args[1]))
        finally:
            try:
                cursor.close()
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  

    def destroy(self):
        self.isDestroying = True
        with self.condition:
            self.condition.notifyAll()
        self.pool.close()
        LOG.info("PersistentPriorityQueue destroyed")
