from nova import config
import sys
import MySQLdb
from oslo.config import cfg


CONF = cfg.CONF
scheduler_opts = [
    cfg.StrOpt("mysql_scheduler_db", default = "scheduler_priority_queue", help = "the mysql database implementing the priority queue"),
    cfg.StrOpt("mysql_user", default = "root", help = "the mysql user"),
    cfg.StrOpt("mysql_passwd", default = "admin", help = "the mysql password"),
    cfg.StrOpt("mysql_host", default="localhost",  help = "the mysql host")
]

cfg.CONF.register_opts(scheduler_opts)
cfg.CONF.import_opt('cpu_allocation_ratio', 'nova.scheduler.filters.core_filter')


class Status(object):
    def __init__(self, mysqlHost, mysqlUser, mysqlPasswd, mysqlDB, vcpuRatio):
        if not mysqlHost:
            raise Exception("mysqlHost not defined!")

        if not mysqlUser:
            raise Exception("mysqlUser not defined!")

        if not mysqlPasswd:
            raise Exception("mysqlPasswd not defined!")

        self.mysqlHost = mysqlHost
        self.mysqlUser = mysqlUser
        self.mysqlPasswd = mysqlPasswd
        self.mysqlDB = mysqlDB
        self.vcpuRatio = vcpuRatio


    def getResourceData(self):
        resources = {}
        dbConnection = None
        resources = {"id" : 0,
                     "vcpus_used": 0,
                     "vcpus_available": 0,
                     "running_vms": 0
        }


        try:
            dbConnection = MySQLdb.connect(self.mysqlHost, self.mysqlUser, self.mysqlPasswd)
            cursor = dbConnection.cursor()
            cursor.execute("""select ncn.id, ncn.vcpus, ncn.vcpus_used, ncn.running_vms from nova.compute_nodes as ncn where ncn.deleted_at is NULL""")

            for item in cursor.fetchall():
                resources["id"] += item[0]
                resources["vcpus_used"] += item[2]
                resources["vcpus_available"] += ((item[1]*self.vcpuRatio)-item[2])
                resources["running_vms"] += item[3]

            cursor.execute("""select count(*) from %s.priority_queue""" % self.mysqlDB)

            item = cursor.fetchone()

            resources["request_in_queue"] = item[0]
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
        finally:
            try:
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  

        return resources


    
if __name__ == '__main__': 
    cfg.CONF(sys.argv[1:], default_config_files=["/etc/nova/nova.conf"])

    if not cfg.CONF.config_file:
        sys.exit("ERROR: Unable to find configuration file via the default search paths (/etc/nova/nova.conf) and the '--config-file' option!")

    status = Status(mysqlHost=cfg.CONF.mysql_host,
                         mysqlUser=cfg.CONF.mysql_user,
                         mysqlPasswd=cfg.CONF.mysql_passwd,
                         mysqlDB=cfg.CONF.mysql_scheduler_db,
                         vcpuRatio=cfg.CONF.cpu_allocation_ratio);

    computeResourceStatus = status.getResourceData()



    msg = "\n---------------------------------------------------------------------------\n"
    msg += '{0:10s}| {1:20s}| {2:20s}| {3:20s}\n'.format("VCPU USED", "VCPUAVAILAIBLE", "RUNNING_VMS", "REQUEST IN QUEUE")
    msg += "---------------------------------------------------------------------------\n"

    msg += "{0:10s}| {1:20s}| {2:20s}| {3:20s}\n".format(str(computeResourceStatus["vcpus_used"]), str(computeResourceStatus["vcpus_available"]), str(computeResourceStatus["running_vms"]), str(computeResourceStatus["request_in_queue"]))

    msg += "---------------------------------------------------------------------------\n"

    print msg

    
    
        
