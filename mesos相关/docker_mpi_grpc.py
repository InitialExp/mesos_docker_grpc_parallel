import os, traceback, uuid
import sys
import time
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from google.protobuf.json_format import MessageToDict, MessageToJson
TOTAL_TASKS = 4 #
TASK_CPUS = 1
TASK_MEMS = 128


import socket
from contextlib import closing

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
     s.bind(('', 0))
     return s.getsockname()[1]


class TestScheduler(mesos.interface.Scheduler):
    def __init__(self):
        self.imageName = "node1:5000/mpich:v1.7"
        self.numInstances = TOTAL_TASKS
        self.pendingInstances = []
        self.runningInstances = []
        self.taskLaunched = 0

        self.name_list = []

        self.grpc_started = False
        self.grpc_port    = None

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value
        with open("/root/docker_study/test_data/frameworkId", "w") as f:
            f.write(frameworkId.value)

    def runTaskOnNode2(self, driver):
        pass


    def resourceOffers(self, driver, offers):
        tasks = []
        for offer in offers:
            print "Considering resource offer %s from %s" % (offer.id.value, offer.hostname)

            offerCpus = 0
            offerMems = 0

            port_begin = 0
            port_end   = 0
            for resouce in offer.resources:
                if resouce.name == "cpus":
                    offerCpus += resouce.scalar.value
                elif resouce.name == "mem":
                    offerMems += resouce.scalar.value
                elif resouce.name == "ports":
                    port_begin =  resouce.ranges.range[0].begin
                    port_end   =  resouce.ranges.range[0].end
            print "Received offer %s of %s with cpus: %s and mem: %s" % (offer.id.value, offer.hostname, offerCpus, offerMems)


            leftCPUs = offerCpus
            leftMems = offerMems
            if self.grpc_started is False and offer.hostname=="node1" and offerCpus>0.5 and offerMems>128:

                self.pendingInstances.append("master_p")
                print(self.pendingInstances)
                task = mesos_pb2.TaskInfo()
                task.task_id.value = "master_p"
                task.slave_id.value = offer.slave_id.value
                task.name = "master_p"

                dockerinfo = mesos_pb2.ContainerInfo.DockerInfo()
                dockerinfo.image = self.imageName
                dockerinfo.network = 4
                dockerinfo.force_pull_image = False

                docker_parameters = dockerinfo.parameters.add()
                docker_parameters.key = "interactive"
                docker_parameters.value = "true"

                port_mapping = dockerinfo.port_mappings.add()
                port_mapping.host_port = port_begin
                port_mapping.container_port = 50051


                containerinfo = mesos_pb2.ContainerInfo()
                containerinfo.type = 1
                # containerinfo.hostname = "node%d" % tid

                networkinfo = containerinfo.network_infos.add()
                networkinfo.name = "my-overlay1"
                # ip_address = networkinfo.ip_addresses.add()
                # ip_address.ip_address = "10.0.1.02%d" % tid
                volume = containerinfo.volumes.add()
                volume.container_path = "/test_data"
                volume.host_path = "/root/docker_study/test_data"
                volume.mode = 1

                volume_trading = containerinfo.volumes.add()
                volume_trading.container_path = "/trading_data"
                volume_trading.host_path = "/root/trading_data"
                volume_trading.mode = 1

                # ttyinfo = mesos_pb2.TTYInfo()
                # ttyinfo.window_size.rows = 200
                # ttyinfo.window_size.columns = 200

                command = mesos_pb2.CommandInfo()
                command.value = '/usr/sbin/service ssh restart && sleep 600'
                command.shell=True
                task.command.MergeFrom(command)

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = 0.5

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = 128

                port_r = task.resources.add()
                port_r.name = "ports"
                port_r.type=mesos_pb2.Value.RANGES

                range_s = mesos_pb2.Value.Range()
                range_s.begin = port_begin
                range_s.end = port_begin
                self.grpc_port = port_begin
                port_r.ranges.range.append(range_s)

                # containerinfo.tty_info.MergeFrom(ttyinfo)

                containerinfo.docker.MergeFrom(dockerinfo)
                task.container.MergeFrom(containerinfo)
                tasks.append(task)
                self.taskLaunched += 1


            elif self.grpc_started is True and len(self.pendingInstances)+len(self.runningInstances)<=TOTAL_TASKS+1 and\
                    offer.hostname=="node2":
                while leftCPUs>=TASK_CPUS and leftMems >= TASK_MEMS:
                    tid = self.taskLaunched

                    task = mesos_pb2.TaskInfo()
                    task.task_id.value = str(tid)
                    task.slave_id.value = offer.slave_id.value
                    task.name = "task_%d" % self.taskLaunched
                    self.pendingInstances.append(str(tid))

                    dockerinfo = mesos_pb2.ContainerInfo.DockerInfo()
                    dockerinfo.image = self.imageName
                    dockerinfo.network = 4
                    dockerinfo.force_pull_image = False

                    docker_parameters = dockerinfo.parameters.add()
                    docker_parameters.key = "interactive"
                    docker_parameters.value = "true"



                    containerinfo = mesos_pb2.ContainerInfo()
                    containerinfo.type = 1
                    # containerinfo.hostname = "node%d" % tid

                    networkinfo = containerinfo.network_infos.add()
                    networkinfo.name = "my-overlay1"
                    # ip_address = networkinfo.ip_addresses.add()
                    # ip_address.ip_address = "10.0.1.02%d" % tid
                    volume = containerinfo.volumes.add()
                    volume.container_path = "/test_data"
                    volume.host_path = "/root/docker_study/test_data"
                    volume.mode = 1

                    volume_trading = containerinfo.volumes.add()
                    volume_trading.container_path = "/trading_data"
                    volume_trading.host_path = "/root/trading_data"
                    volume_trading.mode = 1

                    # ttyinfo = mesos_pb2.TTYInfo()
                    # ttyinfo.window_size.rows = 200
                    # ttyinfo.window_size.columns = 200

                    command = mesos_pb2.CommandInfo()
                    command.value = '/usr/sbin/service ssh restart && sleep 600'
                    command.shell = True
                    task.command.MergeFrom(command)

                    cpus = task.resources.add()
                    cpus.name = "cpus"
                    cpus.type = mesos_pb2.Value.SCALAR
                    cpus.scalar.value = TASK_CPUS

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = TASK_MEMS

                    # containerinfo.tty_info.MergeFrom(ttyinfo)

                    containerinfo.docker.MergeFrom(dockerinfo)
                    task.container.MergeFrom(containerinfo)
                    tasks.append(task)
                    self.taskLaunched += 1
                    leftCPUs -= TASK_CPUS
                    leftMems -= TASK_MEMS


            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)
            driver.acceptOffers([offer.id], [operation])
            driver.reviveOffers()








            # operation = mesos_pb2.Offer.Operation()
            # operation.type = mesos_pb2.Offer.Operation.LAUNCH
            # operation.launch.task_infos.extend(tasks)
            # driver.acceptOffers([offer.id], [operation])


    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))
        if update.state == mesos_pb2.TASK_LOST or \
                update.state == mesos_pb2.TASK_KILLED or \
                update.state == mesos_pb2.TASK_FINISHED or \
                update.state == mesos_pb2.TASK_ERROR or \
                update.state == mesos_pb2.TASK_FAILED:
            try:
                self.pendingInstances.remove(update.task_id.value);
                print(self.pendingInstances)
                self.runningInstances.remove(update.task_id.value);


                if update.task_id.value == "master_p":
                    self.grpc_started = False
                    self.grpc_name = None
                else:
                    container_status = update.container_status
                    container_status_dict = MessageToDict(container_status, preserving_proto_field_name=True)

                    name = "mesos-" + container_status_dict['container_id']['value'].encode('utf-8')
                    self.name_list.remove(name)
            except Exception, e:
                pass
        if update.state == mesos_pb2.TASK_RUNNING:
            print(self.pendingInstances)
            try:
                self.pendingInstances.remove(update.task_id.value)
            except Exception as e:
                traceback.print_exc()

            self.runningInstances.append(update.task_id.value)


            container_status =  update.container_status
            container_status_dict = MessageToDict(container_status, preserving_proto_field_name=True)
            name = "mesos-" + container_status_dict['container_id']['value'].encode('utf-8')
            if update.task_id.value == "master_p":
                self.grpc_started = True
                self.grpc_name = name
                with open("/root/docker_study/test_data/grpc_port", "w") as f:
                    f.write(str(self.grpc_port))
            else:
                self.name_list.append(name)


            if len(self.name_list) == self.numInstances:
                with open("/root/docker_study/test_data/masterContainer", "w") as f:
                    f.write(self.grpc_name)

                with open("/root/docker_study/test_data/host", "w") as f:
                    print (self.grpc_name)
                    f.write(self.grpc_name)
                    f.write("\n")
                    for name_str in self.name_list:
                        f.write(name_str)
                        f.write("\n")

        print 'Number of instances: pending=' + str(len(self.pendingInstances)) + ", runningInstances=" + str(
            len(self.runningInstances))




if __name__ == "__main__":
    framework = mesos_pb2.FrameworkInfo()
    framework.user = "root" # Have Mesos fill in the current user.
    framework.name = "|Test-parameter-tuning" + "|user:sun|uid:"+str(uuid.uuid1())+"|"
    framework.checkpoint = True
    os.system("ssh node1 docker container prune -f")
    os.system("ssh node2 docker container prune -f")


    driver = mesos.native.MesosSchedulerDriver(TestScheduler(),framework,"172.26.8.130:5050")
    print("framework id is:", framework.id.value)



    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    driver.stop();
    sys.exit(status)