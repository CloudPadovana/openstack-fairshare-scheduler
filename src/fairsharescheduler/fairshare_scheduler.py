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

import datetime
from oslo.config import cfg
from oslo import messaging
import random
from threading import Timer
import threading
import MySQLdb
import time
import signal
import json

from nova import rpc
from nova import exception
from nova.openstack.common import importutils
from nova.compute import rpcapi as compute_rpcapi
#from nova.openstack.common import rpc as rpc_ampq
from nova.openstack.common import log as logging
from nova.openstack.common.gettextutils import _
#from nova.openstack.common.rpc import amqp
from oslo.messaging._drivers import amqp
from nova.pci import pci_request
from nova.scheduler import driver, scheduler_options, utils as scheduler_utils

from fairshare_manager import FairShareManager
from persistent_priority_request_queue import PersistentPriorityRequestQueue

__author__ = "Eric Frizziero, Lisa Zangrando"
__email__ = "eric.frizziero[AT]pd.infn.it, lisa.zangrando[AT]pd.infn.it"


CONF = cfg.CONF
LOG = logging.getLogger(__name__)
#CONF.import_opt('cpu_allocation_ratio', 'nova.scheduler.filters.core_filter')


filter_scheduler_opts = [
    cfg.IntOpt('scheduler_host_subset_size',
               default = 1,
               help = 'New instances will be scheduled on a host chosen '
                    'randomly from a subset of the N best hosts. This '
                    'property defines the subset size that a host is '
                    'chosen from. A value of 1 chooses the '
                    'first host returned by the weighing functions. '
                    'This value must be at least 1. Any value less than 1 '
                    'will be ignored, and 1 will be used instead'),
                         
    cfg.IntOpt('num_of_periods',
               default = 3,
               help = 'the number of periods considered the fair-share algorithm'),
                         
    cfg.IntOpt('period_length',
               default = 7,
               help = 'the length (days) of the period (e.g. 7 days)'),
                         
    cfg.IntOpt('rate',
               default = 1,
               help = 'how often the fair-share algorithm has to run (default "5" minutes)'),
                         
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
                         
    cfg.IntOpt('thread_pool_size',
               default = 5,
               help = 'the number of threads consuming the user requests'),
                         
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
                
keystone_opts = [
    cfg.StrOpt('admin_user', required=True),
    cfg.StrOpt('admin_password', required=True),
    cfg.StrOpt('admin_tenant_name', required=True),
    cfg.StrOpt('auth_uri', required=True)
]

CONF.register_opts(filter_scheduler_opts)
CONF.register_opts(keystone_opts, group="keystone_authtoken")

class WorkerConsumer(threading.Thread):
    def __init__(self, threadID, name, queue, method, noComputeResourceCondition, resources):
        threading.Thread.__init__(self)
        super(WorkerConsumer, self).setDaemon(True)
        self.threadID = threadID
        self.name = name
        self.method = method
        self.noComputeResourceCondition = noComputeResourceCondition
        self.resources = resources
        self.queue = queue
        if not queue:
            raise Exception("queue not defined!")

    def run(self):
        exitRun = False

        while not exitRun:
            try:
                requestOK = False
                vcpusRequired = 0

                LOG.info("%s (before queue.getRequest) vcpus_available %s" % (self.name, self.resources['vcpus_available']))
                request = self.queue.getRequest()
                LOG.info("%s dopo getRequest" % self.name)

                if request is not None:
                    while not requestOK:
                        with self.noComputeResourceCondition:
                            LOG.info("%s (inside noComputeResourceCondition)  vcpus_available %s" % (self.name, self.resources['vcpus_available']))

                            if self.resources['vcpus_available'] <= 0:
                                LOG.info("%s waiting..." % self.name)
                                self.noComputeResourceCondition.wait();
                            else:
                                request_spec = request["request_spec"]
                                vcpusRequired = request_spec["num_instances"] * request_spec["instance_type"]["vcpus"]
                                context = amqp.unpack_context(CONF, request["message_context"])

                                LOG.info("%s vcpus_available %s" % (self.name, self.resources['vcpus_available']))
                                self.resources['vcpus_available'] -= vcpusRequired

                                LOG.info("%s vcpus_available updated %s" % (self.name, self.resources['vcpus_available']))
                                requestOK = True
                                self.noComputeResourceCondition.notifyAll()

                    LOG.info("%s user_id=%s project_id=%s timestamp=%s priority=%s (%s vcpus available)" % (self.name, request["userId"], request["projectId"], request["timestamp"], request["priority"], self.resources['vcpus_available']))

                    if (self.method(context=context, request=request)):
                        self.queue.deleteRequestDB(request["idRecord"])

                    self.queue.task_done()
                else:
                    exitRun = True
            except exception.InstanceNotFound as ex:
                LOG.info("WorkerConsumer InstanceNotFound error: type=%s ex=%s" % (type(ex), ex))

                self.queue.deleteRequestDB(request["idRecord"])
                self.queue.task_done()

                with self.noComputeResourceCondition:
                    self.resources['vcpus_available'] += vcpusRequired
                    self.noComputeResourceCondition.notifyAll()

            except Exception as ex:
                LOG.info("WorkerConsumer error: type=%s ex=%s" % (type(ex), ex))

        LOG.info("Exiting %s" % self.name)


class ThreadWorkerGroup(object):
    def __init__(self, queue, method, noResourceCondition, resources, thread_pool_number):
        self.queue = queue
        self.thread_pool_number= thread_pool_number
        self.threadPool = []
        # Create new worker threads
        for workerId in range(self.thread_pool_number):
            workerConsumer = WorkerConsumer(workerId, "WorkerConsumer-" + str(workerId), queue, method, noResourceCondition, resources)
            self.threadPool.append(workerConsumer)
            LOG.info("%s created" % workerConsumer.name)


    def startPool(self):
        for t in self.threadPool:
            t.start()
            LOG.info("%s started" % t.name)

    def stopAndRemovePool(self):
        self.queue.join()
        LOG.info("queue join done!")




class NotificationEndpoint(object):
    def __init__(self, noComputeResourceCondition, computeResources):
        self.noComputeResourceCondition = noComputeResourceCondition
        self.computeResources = computeResources

    def info(self, ctxt, publisher_id, event_type, payload, metadata):
        #LOG.info("NotificationEndpoint INFO: event_type=%s payload=%s metadata=%s" % (event_type, payload, metadata))
        
        if payload is None or not payload.has_key("state"):
           return

        state = payload["state"]
        instanceId = payload["instance_id"]

        LOG.info("NotificationEndpoint INFO: event_type=%s state=%s instanceId=%s" % (event_type, state, instanceId))

        if (event_type == "compute.instance.delete.end" and state == "deleted") or (event_type == "compute.instance.create.error" and state == "building") or (event_type == "scheduler.run_instance" and state == "error"):
            memory_mb = 0
            vcpus = 0

            if event_type == "scheduler.run_instance":
                memory_mb = payload["request_spec"]["instance_type"]["memory_mb"]
                vcpus = payload["request_spec"]["instance_type"]["vcpus"]
            else:
                memory_mb = payload["memory_mb"]
                vcpus = payload["vcpus"]

            with self.noComputeResourceCondition:
                self.computeResources["vcpus_available"] += vcpus
                self.noComputeResourceCondition.notifyAll()

            LOG.info("notify catched: event_type=%s memory_mb=%s vcpus=%s vcpus_available=%s" %(event_type, memory_mb, vcpus, self.computeResources["vcpus_available"])) #example event_type=compute.instance.delete.end memory_mb=512 vcpus=1


    def warn(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.info("NotificationEndpoint WARN: event_type=%s payload=%s metadata=%s" % (event_type, payload, metadata))
        state = payload["state"]
        instanceId = payload["instance_id"]
        LOG.info("NotificationEndpoint WARN: event_type=%s state=%s instanceId=%s" % (event_type, state, instanceId))


    def error(self, ctxt, publisher_id, event_type, payload, metadata):
        LOG.info("NotificationEndpoint ERROR: %s" % (payload))





class FairShareScheduler(driver.Scheduler):
    """Fair-share scheduler that can be used for filtering and weighing."""

    def __init__(self, *args, **kwargs):
        super(FairShareScheduler, self).__init__(*args, **kwargs)
        self.options = scheduler_options.SchedulerOptions()
        self.compute_rpcapi = compute_rpcapi.ComputeAPI()
        #self.notifier = notifier.get_notifier('scheduler')
        self.notifier = rpc.get_notifier('scheduler')

        if CONF.scheduler_max_attempts < 1:
            raise exception.NovaException(_("Invalid value for 'scheduler_max_attempts', must be >= 1"))

        projectShares = {}
        projectSharesCfg = cfg.CONF.project_shares

        if projectSharesCfg:
            projectSharesCfg = projectSharesCfg.replace("'", "\"")
            projectShares = json.loads(projectSharesCfg)

        userShares = {}
        userShareCfg = cfg.CONF.user_shares

        if userShareCfg:
            userShareCfg = userShareCfg.replace("'", "\"")
            userShares = json.loads(userShareCfg)
            #userShares = dict([(str(k), v) for k, v in userShares.items()])

        """ 
        projectShares = {}
        for share in CONF.project_shares:
            x = share.split(":")
            projectShares[x[0]] = float(x[1])

        userShares = {}
        for share in CONF.user_shares:
            x = share.split(":")
            userShares[x[0]] = float(x[1])
        """

        self.vcpuRatio = CONF.cpu_allocation_ratio
        self.computeResources = {}
        self.noComputeResourceCondition = threading.Condition()
     
        self.queue = PersistentPriorityRequestQueue(mysqlHost=CONF.mysql_host, mysqlUser=CONF.mysql_user, mysqlPasswd=CONF.mysql_passwd, mysqlDatabase=CONF.mysql_scheduler_db, poolSize=CONF.mysql_pool_size)

        self.keystone_admin_user = CONF.keystone_authtoken.admin_user
        self.keystone_admin_password = CONF.keystone_authtoken.admin_password
        self.keystone_admin_project_name = CONF.keystone_authtoken.admin_tenant_name
        self.keystone_auth_url = CONF.keystone_authtoken.auth_uri

        self.faireShareManager = FairShareManager(mysqlHost=CONF.mysql_host, mysqlUser=CONF.mysql_user, mysqlPasswd=CONF.mysql_passwd,
                                                  numOfPeriods=CONF.num_of_periods, periodLength=CONF.period_length, defaultProjectShare=CONF.default_project_share,
                                                  projectShares=projectShares, userShares=userShares, decayWeight=CONF.decay_weight,
                                                  vcpusWeight=CONF.fair_share_vcpus_weight, memoryWeight=CONF.fair_share_memory_weight,
                                                  keystone_admin_user=self.keystone_admin_user, keystone_admin_password=self.keystone_admin_password,
                                                  keystone_admin_project_name=self.keystone_admin_project_name, keystone_auth_url=self.keystone_auth_url)
        self.__priorityUpdater()
        
        #self.conn = rpc.create_connection(new=True)
        self.computeResources = self.__getComputeResourceAvailable(mysqlHost=CONF.mysql_host, mysqlUser=CONF.mysql_user, mysqlPasswd=CONF.mysql_passwd, vcpuRatio=self.vcpuRatio)

        thread_pool_size = CONF.thread_pool_size
        if thread_pool_size < 1:
            thread_pool_size = 5
            
        self.threadPool = ThreadWorkerGroup(self.queue, self.schedule_instance, self.noComputeResourceCondition, self.computeResources, thread_pool_size)
        self.threadPool.startPool()

        transport = messaging.get_transport(cfg.CONF)

        targets = [
            #messaging.Target(topic='notifications.info'),
            messaging.Target(topic='notifications'),
        ]

        endpoints = [
            NotificationEndpoint(self.noComputeResourceCondition, self.computeResources),
            #ErrorEndpoint(),
        ]

        server = messaging.get_notification_listener(transport, targets, endpoints, executor="eventlet")
        server.start()
        #server.wait()

        signal.signal(signal.SIGALRM, self.destroy)

    """
    def notify(self, message_data, **kwargs):
        #LOG.info(">>>>>>>>>>>>> notify!!!! message_data=%s args=%s" %(message_data, kwargs))
        event_type = message_data["event_type"]
        state = message_data["payload"]["state"]
        instanceId = message_data["payload"]["instance_id"]
        #LOG.info(">>>>> notify!!!! event_type=%s state=%s instanceId=%s" % (event_type, state, instanceId))
        
        if (event_type == "compute.instance.delete.end" and state == "deleted") or (event_type == "compute.instance.create.error" and state == "building") or (event_type == "scheduler.run_instance" and state == "error"):            
            memory_mb = 0
            vcpus = 0
            
            if event_type == "scheduler.run_instance":
                memory_mb = message_data["payload"]["request_spec"]["instance_type"]["memory_mb"]
                vcpus = message_data["payload"]["request_spec"]["instance_type"]["vcpus"]
            else:
                memory_mb = message_data["payload"]["memory_mb"]
                vcpus = message_data["payload"]["vcpus"]
              
            with self.noComputeResourceCondition:
                self.computeResources["vcpus_available"] += vcpus
                self.noComputeResourceCondition.notifyAll()
               
            LOG.info("notify catched: event_type=%s memory_mb=%s vcpus=%s vcpus_available=%s" %(event_type, memory_mb, vcpus, computeResources["vcpus_available"])) #example event_type=compute.instance.delete.end memory_mb=512 vcpus=1
    """

    def __getComputeResourceAvailable(self, mysqlHost, mysqlUser, mysqlPasswd, vcpuRatio):
        resources = {}
        try:
            dbConnection = MySQLdb.connect(mysqlHost, mysqlUser, mysqlPasswd)
            cursor = dbConnection.cursor()
            cursor.execute("""select sum(ncn.vcpus) as vcpu, sum(ncn.vcpus_used) as vcpus_used, sum(ncn.running_vms) as running_vms from nova.compute_nodes as ncn where ncn.deleted_at is NULL""")
            item = cursor.fetchone()
            resources = { "vcpus_available": int(float(item[0])*vcpuRatio-float(item[1])), "running_vms": int(item[2]) }

        except MySQLdb.Error, e:
            LOG.error("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            try:
                dbConnection.close()
            except MySQLdb.Error, e:
                pass  
        
        LOG.info("__getComputeResourceAvailable resources %s" % resources)
        return resources
    
    
    def __existInstance(self, mysqlHost, mysqlUser, mysqlPasswd, uuid):
        exist = False
        try:
            dbConnection = MySQLdb.connect(mysqlHost, mysqlUser, mysqlPasswd)
            cursor = dbConnection.cursor()
            cursor.execute("""select id from nova.instances where deleted_at IS NULL and terminated_at IS NULL and uuid=%s""", [uuid])
            item = cursor.fetchone()
            if item:
                exist=True
        except MySQLdb.Error, e:
            LOG.error("Error %d: %s" % (e.args[0], e.args[1]))
        finally:
            try:
                dbConnection.close()
            except MySQLdb.Error, e:
                pass

        LOG.info("__existInstance %s" % exist)
        return exist


    def __priorityUpdater(self):
        self.faireShareManager.calculateFairShares()
        self.queue.priorityUpdater(self.getPriority)
        self.priorityUpdaterTimer = Timer(CONF.rate * 60.0, self.__priorityUpdater)
        self.priorityUpdaterTimer.start()


    def getPriority(self, userId, projectId, timestamp, retryCount=0):
        fairShareVcpus, fairShareMemory = self.faireShareManager.getFairShare(userId, projectId)

        if not timestamp:
            timestamp = datetime.datetime.now()
        elif not isinstance(timestamp, datetime.datetime):
            # ISO 8601 extended time format with microseconds
            ISO8601_TIME_FORMAT_SUBSECOND = '%Y-%m-%dT%H:%M:%S.%f'
            timestamp = datetime.datetime.strptime(timestamp, ISO8601_TIME_FORMAT_SUBSECOND)
 
        now = datetime.datetime.now()
        diff = (now - timestamp)
        minutes = diff.seconds / 60 #convert days to minutes
        priority = float(CONF.age_weight) * minutes + float(CONF.fair_share_vcpus_weight) * fairShareVcpus + float(CONF.fair_share_memory_weight) * fairShareMemory - float(CONF.age_weight) * retryCount
        
        LOG.info("PRIORITY %s for userId %s projectId %s" % (priority, userId, projectId))

        return int(-priority)
       
       
    def destroy(self):
        self.priorityUpdaterTimer.cancel()
        self.faireShareManager.destroy()
        
        self.queue.destroy()
        with self.noComputeResourceCondition:
            self.noComputeResourceCondition.notifyAll()
        self.threadPool.stopAndRemovePool()
        try:
            self.conn.close()
        except Exception:
            pass
        #self.server.stop()

    def schedule_run_instance(self, context, request_spec, admin_password, injected_files, requested_networks, is_first_time, filter_properties, legacy_bdm_in_spec):
        message_context = {}
        amqp.pack_context(message_context, context)
        
        instance_properties = request_spec["instance_properties"]
        userId = instance_properties["user_id"]
        projectId = instance_properties["project_id"]
        timestamp = instance_properties["created_at"]
        instance_uuids = request_spec["instance_uuids"]
        
        for uuid in instance_uuids:
            request_spec["instance_uuids"] = [uuid]
            request_spec["num_instances"] = 1
            
            request = { "message_context": message_context,
                        "request_spec": request_spec,
                        "userId": userId,
                        "projectId": projectId,
                        "timestamp": timestamp,
                        "admin_password": admin_password,
                        "injected_files": injected_files,
                        "requested_networks": requested_networks,
                        "is_first_time": is_first_time,
                        "filter_properties": filter_properties,
                        "legacy_bdm_in_spec": legacy_bdm_in_spec }
            
            self.queue.putRequest(request=request, priority=self.getPriority(userId, projectId, timestamp))


    def schedule_instance(self, context, request):
        """ First create a build plan (a list of WeightedHosts) and then provision.
        Returns a list of the instances created.
        """

        request_spec = request["request_spec"]
        filter_properties = request["filter_properties"]
        instance_uuids = request_spec["instance_uuids"]

        if not self.__existInstance(CONF.mysql_host, CONF.mysql_user, CONF.mysql_passwd, instance_uuids[0]):
            raise exception.InstanceNotFound("instance_uuids=%s not found!" % instance_uuids)

        payload = dict(request_spec=request_spec)
        self.notifier.info(context, 'scheduler.run_instance.start', payload)

        LOG.info("Attempting to build %(num_instances)d instance(s) uuids: %(instance_uuids)s" % {'num_instances': len(instance_uuids), 'instance_uuids': instance_uuids})
        LOG.debug("Request Spec: %s" % request_spec)

        found = False
        count = 0
        maxRetryCount = 30
        try:
            while not found and count < maxRetryCount:
                weighed_hosts = self.__schedule(context, request_spec, filter_properties, instance_uuids)
                if weighed_hosts and len(weighed_hosts) > 0:
                    found = True
                else:
                    LOG.info("sleeping 3 sec (count=%s/%s)" % (count, maxRetryCount))
                    time.sleep(3)
                count += 1
        except Exception as ex:
            LOG.error(ex)

        #LOG.info("schedule_run_instance2:self weighed_hosts=%s" % weighed_hosts)

        # NOTE: Pop instance_uuids as individual creates do not need the
        # set of uuids. Do not pop before here as the upper exception
        # handler fo NoValidHost needs the uuid to set error state
        instance_uuids = request_spec.pop('instance_uuids')

        # NOTE(comstud): Make sure we do not pass this through. It
        # contains an instance of RpcContext that cannot be serialized.
        filter_properties.pop('context', None)
       
        if len(weighed_hosts) == 0:
            request["retryCount"] = request["retryCount"] + 1
            request["timestamp"] = datetime.datetime.now()
            request["priority"] = self.getPriority(request["userId"], request["projectId"], request["timestamp"], request["retryCount"])

            self.queue.reInsertRequest(request=request)

            with self.noComputeResourceCondition:
                self.computeResources["vcpus_available"] += request_spec["num_instances"] * request_spec["instance_type"]["vcpus"]
                self.noComputeResourceCondition.notifyAll()

            return False
 
        for num, instance_uuid in enumerate(instance_uuids):
            request_spec['instance_properties']['launch_index'] = num

            try:
                try:
                    weighed_host = weighed_hosts.pop(0)
                    LOG.info("Choosing host %(weighed_host)s for instance %(instance_uuid)s" % {'weighed_host': weighed_host, 'instance_uuid': instance_uuid})
                except IndexError:
                    raise exception.NoValidHost(reason="")
                
                self.__provision_resource(context, weighed_host,
                                         request["request_spec"],
                                         request["filter_properties"],
                                         request["requested_networks"],
                                         request["injected_files"],
                                         request["admin_password"],
                                         request["is_first_time"],
                                         instance_uuid=instance_uuid,
                                         legacy_bdm_in_spec=request["legacy_bdm_in_spec"])
            except Exception as ex:
                # NOTE(vish): we don't reraise the exception here to make sure
                #             that all instances in the request get set to
                #             error properly
                driver.handle_schedule_error(context, ex, instance_uuid, request_spec)
                
            # scrub retry host list in case we're scheduling multiple
            # instances:
            retry = filter_properties.get('retry', {})
            retry['hosts'] = []
            
        self.notifier.info(context, 'scheduler.run_instance.end', payload)

        return True
    
    
    def __schedule(self, context, request_spec, filter_properties, instance_uuids=None):
        """
        Returns a list of hosts that meet the required specs, ordered by their fitness.
        """
        elevated = context.elevated()
        instance_properties = request_spec['instance_properties']
        instance_type = request_spec.get("instance_type", None)   
        force_hosts = filter_properties.get("force_hosts", [])
        force_nodes = filter_properties.get("force_nodes", [])
        scheduler_hints = filter_properties["scheduler_hints"] or {}
        group = scheduler_hints.get('group', None)
        update_group_hosts = False

        
        if group:
            group_hosts = self.group_hosts(elevated, group)
            update_group_hosts = True
            
            if not filter_properties.has_key("group_hosts"):
                filter_properties["group_hosts"] = []
                
            configured_hosts = filter_properties['group_hosts']
            filter_properties['group_hosts'] = configured_hosts + group_hosts

        """Populate filter properties with history of retries for this
        request. If maximum retries is exceeded, raise NoValidHost.
        """
        #LOG.info("_schedule scheduler_max_attempts=%s force_hosts=%s force_nodes=%s" % (filter_properties, force_hosts,force_nodes))
        """
        if CONF.scheduler_max_attempts > 1 and not force_hosts and not force_nodes:
            if filter_properties.has_key("retry"):
                retry = filter_properties["retry"]
                retry['num_attempts'] += 1
                
                LOG.info("_schedule scheduler_max_attempts=%s force_hosts=%s force_nodes=%s retry=%s" % ( CONF.scheduler_max_attempts, force_hosts, force_nodes, retry))
                
                #If the request contained an exception from a previous compute build/resize operation, log it to aid debugging
                
                exc = retry.pop("exc", None)  # string-ified exception from compute (exception info from a previous attempt)
                hosts = retry.get("hosts", None) # previously attempted hosts
        
                if exc and hosts:
                    last_host, last_node = hosts[-1]
                    LOG.error(_("Error from last host: %(last_host)s (node %(last_node)s): %(exc)s"), {"last_host": last_host, "last_node": last_node, "exc": exc}, instance_uuid=instance_uuids[0])
                
                if retry["num_attempts"] > CONF.scheduler_max_attempts:
                    msg = (_("Exceeded max scheduling attempts %(max_attempts)d for instance %(instance_uuid)s") % {"max_attempts": CONF.scheduler_max_attempts, "instance_uuid": instance_uuids[0]})
                    
                    raise exception.NoValidHost(reason=msg)
            else:
                filter_properties["retry"] = {"num_attempts": 1, "hosts": []} # list of compute hosts tried
                LOG.info("_schedule scheduler_max_attempts=%s force_hosts=%s force_nodes=%s retry=1" % (CONF.scheduler_max_attempts, force_hosts, force_nodes))
        """
        
        filter_properties["retry"] = {"num_attempts": 1, "hosts": []} # list of compute hosts tried
        #LOG.info("_schedule scheduler_max_attempts=%s force_hosts=%s force_nodes=%s retry=1" % (CONF.scheduler_max_attempts, force_hosts, force_nodes))
        
        # Save useful information from the request spec for filter processing:
        filter_properties.update({'context': context,
                                  'request_spec': request_spec,
                                  'config_options': self.options.get_configuration(),
                                  'instance_type': instance_type,
                                  'project_id': request_spec['instance_properties']['project_id'],
                                  'os_type': request_spec['instance_properties']['os_type'],
                                  'pci_requests': pci_request.get_pci_requests_from_flavor(request_spec.get('instance_type') or {})
                                })

        #LOG.info("_schedule filter_properties (updated) %s" % filter_properties)
        
        # Find our local list of acceptable hosts by repeatedly
        # filtering and weighing our options. Each time we choose a
        # host, we virtually consume resources on it so subsequent
        # selections can adjust accordingly.

        # Note: remember, we are using an iterator here. So only
        # traverse this list once. This can bite you if the hosts
        # are being scanned in a filter or weighing function.
        hosts = self.host_manager.get_all_host_states(elevated)

        #LOG.info(">>>>>> hosts=%s" % hosts)
        
        selected_hosts = []
        if instance_uuids:
            num_instances = len(instance_uuids)
        else:
            num_instances = request_spec.get('num_instances', 1)
            
        for num in xrange(num_instances):
            # Filter local hosts based on requirements ...
            hosts = self.host_manager.get_filtered_hosts(hosts, filter_properties, index=num)
            
            if not hosts:
                # Can't get any more locally.
                break

            #LOG.info("filtered %(hosts)s" % {'hosts': hosts})

            weighed_hosts = self.host_manager.get_weighed_hosts(hosts, filter_properties)

            #LOG.info("weighed %(hosts)s" % {'hosts': weighed_hosts})

            scheduler_host_subset_size = CONF.scheduler_host_subset_size
            if scheduler_host_subset_size > len(weighed_hosts):
                scheduler_host_subset_size = len(weighed_hosts)
                
            if scheduler_host_subset_size < 1:
                scheduler_host_subset_size = 1

            chosen_host = random.choice(weighed_hosts[0:scheduler_host_subset_size])
            selected_hosts.append(chosen_host)

            # Now consume the resources so the filter/weights
            # will change for the next instance.
            chosen_host.obj.consume_from_instance(instance_properties)
            if update_group_hosts is True:
                filter_properties['group_hosts'].append(chosen_host.obj.host)
                
        return selected_hosts


    def __provision_resource(self, context, weighed_host, request_spec,
            filter_properties, requested_networks, injected_files,
            admin_password, is_first_time, instance_uuid=None,
            legacy_bdm_in_spec=True):
        """Create the requested resource in this Zone."""
        # NOTE(vish): add our current instance back into the request spec
        request_spec['instance_uuids'] = [instance_uuid]
        
        payload = dict(request_spec=request_spec, weighted_host=weighed_host.to_dict(), instance_id=instance_uuid)
        self.notifier.info(context, 'scheduler.run_instance.scheduled', payload)

        # Update the metadata if necessary
        scheduler_hints = filter_properties.get('scheduler_hints') or {}
        group = scheduler_hints.get('group', None)
        values = None

        if group:
            values = request_spec['instance_properties']['system_metadata']
            values.update({'group': group})
            values = {'system_metadata': values}

        try:
            updated_instance = driver.instance_update_db(context, instance_uuid, extra_values=values)
        except exception.InstanceNotFound:
            LOG.warning(_("Instance disappeared during scheduling"), context=context, instance_uuid=instance_uuid)
        else:
            scheduler_utils.populate_filter_properties(filter_properties, weighed_host.obj)

            self.compute_rpcapi.run_instance(context,
                    instance=updated_instance,
                    host=weighed_host.obj.host,
                    request_spec=request_spec,
                    filter_properties=filter_properties,
                    requested_networks=requested_networks,
                    injected_files=injected_files,
                    admin_password=admin_password, is_first_time=is_first_time,
                    node=weighed_host.obj.nodename,
                    legacy_bdm_in_spec=legacy_bdm_in_spec)


    def select_hosts(self, context, request_spec, filter_properties):
        """Selects a filtered set of hosts."""
        instance_uuids = request_spec.get('instance_uuids')
        hosts = [host.obj.host for host in self._schedule(context, request_spec, filter_properties, instance_uuids)]
        if not hosts:
            raise exception.NoValidHost(reason="")
        return hosts

    def select_destinations(self, context, request_spec, filter_properties):
        """Selects a filtered set of hosts and nodes."""
        num_instances = request_spec['num_instances']
        instance_uuids = request_spec.get('instance_uuids')
        selected_hosts = self._schedule(context, request_spec, filter_properties, instance_uuids)

        # Couldn't fulfill the request_spec
        if len(selected_hosts) < num_instances:
            raise exception.NoValidHost(reason='')

        dests = [dict(host=host.obj.host, nodename=host.obj.nodename, limits=host.obj.limits) for host in selected_hosts]
        return dests
    


