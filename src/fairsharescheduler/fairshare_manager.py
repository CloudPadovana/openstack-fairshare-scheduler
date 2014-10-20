# Copyright (c) 2014 INFN - "Istituto Nazionale di Fisica Nucleare" - Italy
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import MySQLdb
import datetime
import threading
import sys
import json
from nova.openstack.common import log as logging
from nova import config
from oslo.config import cfg

__author__ = "Eric Frizziero, Lisa Zangrando"
__email__ = "eric.frizziero[AT]pd.infn.it, lisa.zangrando[AT]pd.infn.it"

LOG = logging.getLogger(__name__)

class FairShareManager(object):

    def __init__(self, mysqlHost, mysqlUser, mysqlPasswd, numOfPeriods=3, periodLength=5, projectShares=None, defaultProjectShare=10.0, userShares=None, decayWeight=0.5, vcpusWeight=10000, memoryWeight=7000):
        if not mysqlHost:
            raise Exception("mysqlHost not defined!")
        
        if not mysqlUser:
            raise Exception("mysqlUser not defined!")
        
        if not mysqlPasswd:
            raise Exception("mysqlPasswd not defined!")
        """ 
        if not projectShares:
            raise Exception("projectShares not defined!")
        """

        self.mysqlHost = mysqlHost
        self.mysqlUser = mysqlUser
        self.mysqlPasswd = mysqlPasswd 
        #self.db = MySQLdb.connect(mysqlHost, mysqlUser, mysqlPasswd)
        self.lock = threading.Lock()
        self.usageTable = {}
        self.exit = False
        self.numOfPeriods = numOfPeriods
        self.periodLength = periodLength
        self.userShares = userShares
        self.projectShares = projectShares
        self.defaultProjectShare = defaultProjectShare
        #self.projectShares["totalShare"] = 0
        self.decayWeight = decayWeight
        self.vcpusWeight = vcpusWeight
        self.memoryWeight = memoryWeight
        """
        for share in self.projectShares.values():
            self.projectShares["totalShare"] += share
        """

    def calculateFairShares(self, userId=None, projectId=None):
        now = datetime.datetime.now()
        usageTable = {}
        
        now -= datetime.timedelta(hours=(1))
        
        for x in xrange(self.numOfPeriods):
            self.__getUsage(usageTable=usageTable, fromDate=str(now - datetime.timedelta(days=(x * self.periodLength))), periodLength=self.periodLength)

        if userId and projectId:
            userName = None
            projectName = None

            if not usageTable.has_key(projectId):
                (userName, projectName) = self.__getUserNameProjectName(userId, projectId)

                if self.projectShares and self.projectShares.has_key(projectName):
                    usageTable[projectId] = {"name":projectName, "share":float(self.projectShares[projectName]), "users":{}}
                else:
                    usageTable[projectId] = {"name":projectName, "share":self.defaultProjectShare, "users":{}}

            users = usageTable[projectId]["users"]
            if not users.has_key(userId):
                if not userName:
                    (userName, projectName) = self.__getUserNameProjectName(userId, projectId)

                #usageTable[projectId]["name"] = projectName
                usageRecord = { "memory":0, "normalizedMemoryUsage":float(0), "vcpus":0, "normalizedVcpusUsage":float(0), "timeUsage":0 }
                users[userId] = {"name":userName, "usageRecords":[]}
                users[userId]["usageRecords"].append(usageRecord)
                
                LOG.info("new user added: userId=%s usageRecord=%s" % (userId, usageRecord))
            
        #totalShare = float(self.projectShares["totalShare"])
        totalMemoryUsage = []
        totalVcpusUsage = []
        totalHistoricalVcpusUsage = 0
        totalHistoricalMemoryUsage = 0
        totalShare = 0

        for x in xrange(self.numOfPeriods):
            totalMemoryUsage.append(0)
            totalVcpusUsage.append(0)
        
        for projectId, project in usageTable.items():
            # check the share for each user and update the usageRecord                
            users = project["users"]
            projectName = project["name"]
            projectShare = project["share"]

            siblingShare = 0
            
            for userId, user in users.items():
                userName = user["name"]
                userShare = 0
                
                if self.userShares and self.userShares.has_key(projectName) and self.userShares[projectName].has_key(userName):
                    userShare = self.userShares[projectName][userName]
                    if userShare > projectShare:
                        userShare = 1
                       
                    siblingShare += userShare
                elif len(users) == 1:
                    siblingShare = projectShare
                    userShare = projectShare
                else:
                    if projectShare > 0:
                        userShare = 1
                        siblingShare += userShare
                    else:
                        userShare = 0
                    
                user.update({"share":userShare})
        
                # calculate the totalMemoryUsage and totalVcpusUsage
                index = 0
                for usageRecord in user["usageRecords"]:
                    totalVcpusUsage[index] += usageRecord["normalizedVcpusUsage"]
                    totalMemoryUsage[index] += usageRecord["normalizedMemoryUsage"]
                    index += 1
            
            project["siblingShare"] = siblingShare
            #project["share"] = projectShare
            totalShare = totalShare + projectShare
            
        
        for x in xrange(self.numOfPeriods):
            decay = self.decayWeight ** x
            totalHistoricalVcpusUsage += decay * totalVcpusUsage[x]
            totalHistoricalMemoryUsage += decay * totalMemoryUsage[x]            
            
        for projectId, project in usageTable.items():
            siblingShare = float(project["siblingShare"])
            projectShare = float(project["share"])
            actualVcpusUsage = 0
            actualMemoryUsage = 0
            
            users = project["users"]
            
            for userId, user in users.items():
                # for each user the normalized share is calculated (0 <= userNormalizedShare <= 1)
                userShare = float(user["share"])
                if projectShare > 0:
                    userNormalizedShare = (userShare / siblingShare) * (projectShare / totalShare)
                else:
                    userNormalizedShare = userShare
                    
                user["normalizedShare"] = userNormalizedShare
                
                # calculate the normalizedVcpusUsage, normalizedMemoryUsage, historicalVcpusUsage and the historicalMemoryUsage
                index = 0
                historicalVcpusUsage = 0
                historicalMemoryUsage = 0
                
                for usageRecord in user["usageRecords"]:
                    decay = self.decayWeight ** index
                    
                    historicalVcpusUsage += decay * usageRecord["normalizedVcpusUsage"]
                    historicalMemoryUsage += decay * usageRecord["normalizedMemoryUsage"]

                    index += 1
                    
                if totalHistoricalVcpusUsage > 0:
                    user["normalizedVcpusUsage"] = historicalVcpusUsage / totalHistoricalVcpusUsage
                else:
                    user["normalizedVcpusUsage"] = historicalVcpusUsage
 
                if totalHistoricalMemoryUsage > 0:
                    user["normalizedMemoryUsage"] = historicalMemoryUsage / totalHistoricalMemoryUsage
                else:
                    user["normalizedMemoryUsage"] = historicalMemoryUsage
                
                actualVcpusUsage += user["normalizedVcpusUsage"]
                actualMemoryUsage += user["normalizedMemoryUsage"]
                
            project["actualVcpusUsage"] = actualVcpusUsage
            project["actualMemoryUsage"] = actualMemoryUsage
        
        for project in usageTable.values():
            actualVcpusUsage = project["actualVcpusUsage"]
            actualMemoryUsage = project["actualMemoryUsage"]
            siblingShare = project["siblingShare"]
            users = project["users"]
            
            for user in users.values():
                share = user["share"]
                
                if share == 0:
                    user["fairShareVcpus"] = 0
                    user["fairShareMemory"] = 0
                    user["effectiveVcpusUsage"] = 0
                    user["effectiveMemoryUsage"] = 0
                    continue
                else:
                    normalizedShare = user["normalizedShare"]
                    normalizedVcpusUsage = user["normalizedVcpusUsage"]
                    normalizedMemoryUsage = user["normalizedMemoryUsage"]
                    
                    effectiveVcpusUsage = normalizedVcpusUsage + ((actualVcpusUsage - normalizedVcpusUsage) * float(share) / float(siblingShare))
                    effectiveMemoryUsage = normalizedMemoryUsage + ((actualMemoryUsage - normalizedMemoryUsage) * float(share) / float(siblingShare))
                    user["effectiveVcpusUsage"] = effectiveVcpusUsage
                    user["effectiveMemoryUsage"] = effectiveMemoryUsage
    
                    if normalizedShare == 0:
                        user["fairShareVcpus"] = 0
                        user["fairShareMemory"] = 0
                    else:
                        user["fairShareVcpus"] = 2 ** (-effectiveVcpusUsage / normalizedShare)
                        user["fairShareMemory"] = 2 ** (-effectiveMemoryUsage / normalizedShare)
        
        self.lock.acquire()
        try:
            self.usageTable.clear()
            self.usageTable.update(usageTable)
        finally:
            self.lock.release()
            
        self.printTable()


    def printTable(self, stdout=False):
        msg = "\n-------------------------------------------------------------------------------------------------------------------------------------------------------\n"
        msg += '{0:10s}| {1:8s}| {2:11s}| {3:14s}| {4:19s}| {5:20s}| {6:19s}| {7:19s}| {8:9s}| {9:10s}\n'.format("USER", "PROJECT", "USER SHARE", "PROJECT SHARE", "FAIR-SHARE (Vcpus)", "FAIR-SHARE (Memory)", "actual vcpus usage", "effec. vcpus usage", "priority", "VMs")
        msg += "-------------------------------------------------------------------------------------------------------------------------------------------------------\n"


        conn = MySQLdb.connect(self.mysqlHost, self.mysqlUser, self.mysqlPasswd)

        for projectId, project in self.usageTable.items():
            vmInstances = 0
            for userId, user in project["users"].items():
                #conn = MySQLdb.connect(self.mysqlHost, self.mysqlUser, self.mysqlPasswd)
                cursor = conn.cursor()
                try:
                    #cursor.execute("select count(*) from nova.instances where vm_state='active' and user_id='"+userId+"'")
                    cursor.execute("select count(*) from nova.instances where terminated_at is null and launched_at is not null and deleted_at is null and user_id='"+userId+"' and project_id='"+projectId+"'")
                    row = cursor.fetchone()
                    vmInstances = row[0]
                except Exception as inst:
                    LOG.error("error=%s" % inst)
                finally:
                    cursor.close()
                    #conn.close()
                    
                msg += "{0:10s}| {1:8s}| {2:11s}| {3:14s}| {4:19s}| {5:20s}| {6:19s}| {7:19s}| {8:9}| {9:10}\n".format(user["name"], project["name"], str(user["share"]) + "%", str(project["share"]) + "%", str(user["fairShareVcpus"]), str(user["fairShareMemory"]), "{0:.1f}%".format(user["normalizedVcpusUsage"]*100), "{0:.1f}%".format(user["effectiveVcpusUsage"]*100), str(int(user["fairShareVcpus"]*self.vcpusWeight + user["fairShareMemory"]*self.memoryWeight)), str(vmInstances))
        
        msg += "-------------------------------------------------------------------------------------------------------------------------------------------------------\n\n"

        if stdout:
            print(msg)
        else:
            LOG.info(msg)
        
        conn.close()

        
    def __getUsage(self, usageTable, fromDate, periodLength):
        LOG.info("getUsage: fromDate=%s periodLength=%s days" % (fromDate, periodLength))
        #print("getUsage: fromDate=%s periodLength=%s days" % (fromDate, periodLength))

        conn = MySQLdb.connect(self.mysqlHost, self.mysqlUser, self.mysqlPasswd)
        cursor = conn.cursor()
        period = str(periodLength)
        try:
            cursor.execute("select ku.id as user, ku.name as username, kp.id as project, kp.name as projectname, ((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at,'" + fromDate + "')>='" + fromDate + "','" + fromDate + "',  ni.terminated_at)) - (IF( (ni.launched_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)),UNIX_TIMESTAMP(ni.launched_at),UNIX_TIMESTAMP(DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)) ))))/60) *ni.memory_mb) as memory_usage,((sum((UNIX_TIMESTAMP(IF(IFNULL(ni.terminated_at,'" + fromDate + "')>='" + fromDate + "','" + fromDate + "',  ni.terminated_at)) - (IF( (ni.launched_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)),UNIX_TIMESTAMP(ni.launched_at),UNIX_TIMESTAMP(DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day)) ))))/60) * ni.vcpus) as vcpu_usage from nova.instances ni, keystone.user ku, keystone.project kp  where ni.user_id=ku.id and kp.id=ni.project_id and ni.launched_at IS NOT NULL and ni.launched_at <='" + fromDate + "' and(ni.terminated_at>=DATE_SUB('" + fromDate + "', INTERVAL '" + period + "' day) OR ni.terminated_at is null) group by ku.name, kp.name")
            projectId = 0
            userId = 0

            #LOG.info("rows=%s" % str(cursor.rowcount))
            for row in cursor.fetchall():
                #LOG.info("row=%s" % row)
                userId = row[0]
                userName = row[1]
                projectId = row[2]
                projectName = row[3]
                projectShare = 0.0

                usageRecord = { "normalizedMemoryUsage":float(row[4]), "normalizedVcpusUsage":float(row[5]) }
                LOG.info("usageRecord=%s" % (usageRecord))

                if self.projectShares and self.projectShares.has_key(projectName):
                    projectShare = self.projectShares[projectName]
                else:
                    projectShare = self.defaultProjectShare

                if not usageTable.has_key(projectId):
                    usageTable[projectId] = {"name": projectName, "share": float(projectShare), "users":{}}
                
                users = usageTable[projectId]["users"]
                if not users.has_key(userId):
                    users[userId] = {"name":userName, "usageRecords":[]}


                #LOG.info("project=%s %s" % (projectId, usageTable[projectId]))
     
                #LOG.info("user=%s usageRecord=%s" % (userName, usageRecord))
                #print ("user=%s usageRecord=%s" % (userName, usageRecord))

                users[userId]["usageRecords"].append(usageRecord)
        except Exception as inst:
            LOG.error("error=%s" % inst)
            print type(inst)  # the exception instance
            print inst.args  # arguments stored in .args
            print inst  # __str__ allows args to printed directly
        finally:
            cursor.close()
            conn.close()

    def __getUserNameProjectName(self, userId, projectId):
        conn = MySQLdb.connect(self.mysqlHost, self.mysqlUser, self.mysqlPasswd)
        userNameProjectName = None
        cursor = conn.cursor()
        try:
            cursor.execute("select ku.name as username, kp.name as projectname from keystone.user ku, keystone.project kp  where ku.id='" + userId + "' and kp.id='" + projectId + "'")
            item = cursor.fetchone()
            userNameProjectName = (item[0], item[1]) 
        except MySQLdb.Error, e:
            LOG.info("Error %d: %s" % (e.args[0],e.args[1]))
        finally:
            try:
                cursor.close()
                conn.close()
            except MySQLdb.Error, e:
                pass  
        return userNameProjectName

    def getFairShare(self, userId, projectId):
        if not userId:
            raise Exception("userId not defined!")
        
        if not projectId:
            raise Exception("projectId not defined!")
        
        """
        if not self.usageTable.has_key(projectId):
            raise Exception("projectId " + projectId + " not found!")
        
        if projectId not in self.projectShares:
            raise Exception("projectId " + projectId + " none share assigned!")
        """     
        if not self.usageTable.has_key(projectId) or not self.usageTable[projectId]["users"].has_key(userId):
            self.calculateFairShares(userId, projectId)
            
        fairShareVcpus = 0
        fairShareMemory = 0
        
        self.lock.acquire()
        try:
            user = self.usageTable[projectId]["users"].get(userId)
            fairShareVcpus = user["fairShareVcpus"]
            fairShareMemory = user["fairShareMemory"]
        finally:
            self.lock.release()
            
        return fairShareVcpus, fairShareMemory
    

    def destroy(self):
        #self.db.close()
        pass
        

if __name__ == '__main__':
    CONF = cfg.CONF(sys.argv[1:], project='nova', version="2013.2.2", default_config_files=None)
    
    filter_scheduler_opts = [
        cfg.IntOpt('num_of_periods',
               default = 3,
               help = 'the number of periods considered the fair-share algorithm'),
                         
        cfg.IntOpt('period_length',
               default = 7,
               help = 'the length (days) of the period (e.g. 7 days)'),
                         
        cfg.IntOpt('age_weight',
               default = 1000,
               help = 'the age weight'),
                         
        cfg.IntOpt('decay_weight',
               default = 0.5,
               help = 'the decay weight'),
                         
        cfg.IntOpt('fair_share_vcpus_weight',
               default = 10000,
               help = 'the fair_share vcpus weight'),
                         
        cfg.IntOpt('fair_share_memory_weight',
               default = 7000,
               help = 'the fair_share memory weight'),
                         
        cfg.StrOpt('mysql_host',
               default = "193.206.210.223",
               help = 'the mysql host'),
                         
        cfg.StrOpt('mysql_scheduler_db',
               default = "scheduler_priority_queue",
               help = 'the mysql database implementing the priority queue'),
                         
        cfg.StrOpt('mysql_user',
               default = "root",
               help = 'the mysql user'),

        cfg.StrOpt('mysql_passwd',
               default = "admin",
               help = 'the mysql password'),
                         
        cfg.IntOpt('mysql_pool_size',
               default = 10,
               help = 'the mysql password'),
                        
        cfg.IntOpt('default_project_share',
               default = 10,
               help = 'the default share'),

        cfg.StrOpt('project_shares',
               default = None,
               help = 'the list of project shares'),
                         
        cfg.StrOpt('user_shares',
               default = None,
               help = 'the list of user shares')
    ]

    cfg.CONF.register_opts(filter_scheduler_opts)
    #cfg.CONF.reload_config_files()
    #cfg.CONF.import_opt('fair_share_vcpus_weight', 'nova.scheduler.fairshare_scheduler')
    cfg.CONF(['--config-file', "/etc/nova/nova.conf"])
    
    projectShares = {}
    projectSharesCfg = cfg.CONF.project_shares

    if projectSharesCfg:
        projectSharesCfg = projectSharesCfg.replace("'", "\"")
        projectShares = json.loads(projectSharesCfg)

        #userShares = dict([(str(k), v) for k, v in userShares.items()])

    userShares = {}
    userShareCfg = cfg.CONF.user_shares

    if userShareCfg:
        userShareCfg = userShareCfg.replace("'", "\"")
        userShares = json.loads(userShareCfg)
        #userShares = dict([(str(k), v) for k, v in userShares.items()])


    """
    print("mysqlHost=%s") % cfg.CONF.mysql_host
    print("mysqlUser=%s") % cfg.CONF.mysql_user
    print("mysqlPasswd=%s") % cfg.CONF.mysql_passwd
    print("numOfPeriods=%s") % cfg.CONF.num_of_periods
    print("periodLength=%s") % cfg.CONF.period_length
    print("projectShares=%s") % projectShares
    print("userShares=%s") % userShares
    vcpusWeight=cfg.CONF.fair_share_vcpus_weight, memoryWeight=cfg.CONF.fair_share_memory_weight
    """
                
    #fsm = FairShareManager(mysqlHost="193.206.210.223", mysqlUser="root", mysqlPasswd="admin", numOfPeriods=3, periodLength=5, projectShares=projectShares, userShares=userShares)
    fsm = FairShareManager(mysqlHost=cfg.CONF.mysql_host, mysqlUser=cfg.CONF.mysql_user, mysqlPasswd=cfg.CONF.mysql_passwd, numOfPeriods=cfg.CONF.num_of_periods, periodLength=cfg.CONF.period_length, projectShares=projectShares, userShares=userShares, defaultProjectShare=cfg.CONF.default_project_share, decayWeight=cfg.CONF.decay_weight, vcpusWeight=cfg.CONF.fair_share_vcpus_weight, memoryWeight=cfg.CONF.fair_share_memory_weight)
    fsm.calculateFairShares()
    fsm.printTable(stdout=True)
    fsm.destroy()
 
