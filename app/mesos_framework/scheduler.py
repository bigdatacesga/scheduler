from mesos.interface import mesos_pb2
import mesos.native
import threading
import logging
from .launcher import MyMesosLauncher
import requests
import registry

# Create a global kvstore client
ENDPOINT = 'http://consul:8500/v1/kv'
DISKS_ENDPOINT = 'http://disks.service.int.cesga.es:5000/resources/disks/v1'

class MyMesosScheduler(mesos.interface.Scheduler):
    # Receives a path to the service to be deployed, with the list of nodes
    # Inspects the offers and when possible calls launcher to deploy a node
    # Writes in the key/value store the disks used and the hostnames
    def __init__(self, implicitAcknowledgements):
        self.implicitAcknowledgements = implicitAcknowledgements
        self.logger = logging.getLogger('mesos')
        self.launcher = MyMesosLauncher()
        self.tasks_queue = []
        registry.connect(ENDPOINT)

    def get_queued_instances(self):
        return self.tasks_queue

    def queue_new_instance(self, instance_id):
        service = registry.Cluster(instance_id)
        nodesList = service.nodes
        print(nodesList)
        instances_list = list()
        for node in nodesList:
            instance = dict()

            # This is only used for logging
            instance["node_id"] = node.node_id

            # Variable to be passed to the launcher
            instance["node_dn"] = str(node)

            # Variables to be used when checking if the node can be launched with an offer
            instance["cpu"] = int(node.cpu)
            instance["mem"] = int(node.mem)

            if node.custom_disks == "True":
                disk_list = node.disks
                instance["custom_disks"] = True
                disks = []
                for disk in disk_list:
                    disks.append(disk.name)
                instance["disks"] = disks
            else:
                instance["num_disks"] = int(node.number_of_disks)
                instance["custom_disks"] = False

            if node.custom_node == "True":
                instance["mesos_node_hostname"] = node.mesos_node_hostname
                instance["custom_node"] = True
            else:
                instance["custom_node"] = False

            # Persist that the node is now in the queue
            node.status = "queued"

            # Add node to the queue
            instances_list.append(instance)

        print(instances_list)
        self.tasks_queue += instances_list

    def match_mesos_node(self, hostname, task_data):
        custom_node_needed = task_data["custom_node"]
        if custom_node_needed:
            return hostname == task_data["mesos_node_hostname"]
        else:
            return True

    def match_disks(self, offer_disks, task_data):
        # offer_disks -> list of disks
        # taskdata -> dictionary of task details
        custom_disks_needed = task_data["custom_disks"]
        if custom_disks_needed:
            # Custom disks are specified
            required_tasks_disks = task_data["disks"]
            # Do we have the asked for disks?
            for disk in required_tasks_disks:
                if disk not in offer_disks:
                    return False
        else:
            # Tasks does not care about specific disks
            required_tasks_disks = task_data["num_disks"]
            # Are there enough disks?
            if len(offer_disks) < required_tasks_disks:
                    return False
        return True


    def get_disk_info(self, node, disk):
        r = requests.get(DISKS_ENDPOINT + "/{}/disks/{}".format(node, disk))
        if r.status_code == 200:
            return r.json()[disk]
        else:
            raise Exception("Can't get disks")

    def set_disk_as_used(self, node, clustername, disk):
        payload = {'status': 'used', 'clustername': clustername, 'node': node}
        r = requests.put(DISKS_ENDPOINT + "/{}/disks/{}".format(node, disk), data=payload)
        if r.status_code != 204:
            raise Exception("Can't set disk as used")


    def set_mesos_used_disks(self, node, disks):
        # node.disks = mesos_disks
        node.set_disks(disks)  # Temporary FIX

    def select_disks(self, task_data, offer_disks):
        custom_disks_needed = task_data["custom_disks"]
        used_disks = []
        if custom_disks_needed:
            # Custom disks are specified
            # Set list of disks as resources needed
            required_tasks_disks = task_data["disks"]
            used_disks = required_tasks_disks
        else:
            # Tasks does not care about specific disks
            # Set number of disks as resource needed

            # Register in kvstore too !!!
            registry.connect(ENDPOINT)
            node = registry.Node(task_data["node_dn"])

            mesos_disks = node.disks
            i = 0
            for disk in mesos_disks:
                used_disks.append(offer_disks[i])
                disk_mesos_name = str(offer_disks[i])
                disk.mesos_name = disk_mesos_name

                # Get info from the disks REST API
                disk_info = self.get_disk_info(node.mesos_node_hostname, disk_mesos_name)

                # Set the disk path and origin so that the docker executor can create the dirs
                disk.origin = disk_info['path'] + "/" + node.node_id
                disk.destination = disk_info['path']

                # Set the mode
                disk.mode = disk_info['mode']

                # Set the disk as used
                self.set_disk_as_used(node.mesos_node_hostname, node.clustername, disk_mesos_name)

                # Increment counter of the disk to get from the mesos offer
                i += 1

        return used_disks

    def remove_used_disks(self, offer_disks, used_disks):
        for disk in used_disks:
            offer_disks.remove(disk)
        return offer_disks

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this mesos_framework. A
        single offer will only contain resources from a single slave.
        Resources associated with an offer will not be re-offered to
        _this_ mesos_framework until either (a) this mesos_framework has rejected
        those resources (see SchedulerDriver::launchTasks) or (b) those
        resources have been rescinded (see Scheduler::offerRescinded).
        Note that resources may be concurrently offered to more than one
        mesos_framework at a time (depending on the allocator being used). In
        that case, the first mesos_framework to launch tasks using those
        resources will be able to use them while the other frameworks
        will have those resources rescinded (or if a mesos_framework has
        already launched tasks with those resources then those tasks will
        fail with a TASK_LOST status and a message saying as much).
        """

        self.logger.info("I received %d offers" % len(offers))
        self.logger.info("There are %d pending tasks" % len(self.tasks_queue))

        def handle_offers(nodes_queue):
            declined = []
            self.logger.info("Queued tasks are %s" % str(nodes_queue))
            # Loop over the offers and see if there's anything that looks good
            for offer in offers:
                offer_cpu = 0
                offer_mem = 0
                offer_was_used = False

                # Decline all offers if no tasks to launch
                if nodes_queue == []:
                    declined.append(offer.id)
                    continue

                # Collect up the CPU and Memory resources from the offer
                for resource in offer.resources:
                    if resource.name == "cpus":
                        offer_cpu = resource.scalar.value
                    if resource.name == "mem":
                        offer_mem = resource.scalar.value
                    offer_disks = None
                    if resource.name == "dataDisks":
                        offer_disks = resource.set.item

                self.logger.info("Offer has => cpu: %d mem: %d disks: %s", offer_cpu, offer_mem, str(offer_disks))

                # Iterate over tasks in queue to see how many can be launched with the given resources
                tasks_to_launch = []
                for task in nodes_queue:
                    # self.logger.info("Attempting to launch task %s with => cpu: %d mem: %d", task["name"], task["cpu"] , task["mem"])
                    if(offer_cpu >= task["cpu"] and
                       offer_mem >= task["mem"] and
                       offer_disks is not None and
                       self.match_disks(offer_disks, task) and
                       self.match_mesos_node(offer.hostname, task)):

                        # Remove the task from the queue
                        nodes_queue.remove(task)

                        # Mark the offer as used so it is not declined
                        offer_was_used = True

                        # Get the node to update info
                        node = registry.Node(task["node_dn"])

                        # Persist slave id
                        node.mesos_slave_id = offer.slave_id.value

                        # Persist slave hostname
                        node.mesos_node_hostname = offer.hostname

                        # Persist offer id
                        node.mesos_offer_id = offer.id

                        self.logger.info("MATCH !! -> Will launch task " + task["node_id"] )

                        # Remove the resources used from the offer
                        offer_cpu -= task["cpu"]  # Decrease cpus used
                        offer_mem -= task["mem"]  # Decrease memory used
                        used_disks = self.select_disks(task, offer_disks)  # Get the disks from the pool
                        task["disks"] = used_disks
                        offer_disks = self.remove_used_disks(offer_disks, used_disks)  # Remove used disks

                        self.logger.info("Task %s used disks %s", task["node_id"], str(used_disks))

                        tasks_to_launch.append(task)

                # If we have any tasks to launch, ask the driver to launch them.
                if tasks_to_launch:
                    self.launcher.launch_nodes(tasks_to_launch, driver, offer.id)
                    #for task in tasks_to_launch:
                        #nodes_queue.remove(task)
                        #self.launcher.launch_node(task["node_dn"], driver, offer.id)

                if not offer_was_used:
                    self.logger.info("Offer was useless")
                    declined.append(offer.id)

            # Decline the offers in batch
            if declined != []:
                self.logger.info("I declined %d offers" % len(declined))
                for declinedOffer in declined:
                    driver.declineOffer(declinedOffer)

        t = threading.Thread(target=handle_offers, args=([self.tasks_queue]))
        t.start()

    #### TASK MANAGEMENT CODER ####
    # Not modified from examples
    # https://github.com/tarnfeld/mesos-python-framework/blob/master/framework.py

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos
        master. A unique ID (generated by the master) used for
        distinguishing this mesos_framework from others and MasterInfo
        with the ip and port of the current master are provided as arguments.
        """
        self.logger.info("Registered mesos_framework %s" % (frameworkId.value))

    def reregistered(self, driver, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos master.
        This is only called when the scheduler has previously been registered.
        MasterInfo containing the updated information about the elected master
        is provided as an argument.
        """

        self.logger.info("Connected with master %s" % (masterInfo.ip))

    def disconnected(self, driver):
        """
        Invoked when the scheduler becomes "disconnected" from the master
        (e.g., the master fails and another is taking over).
        """

        self.logger.info("Disconnected from master")

    def offerRescinded(self, driver, offerId):
        """
        Invoked when an offer is no longer valid (e.g., the slave was
        lost or another mesos_framework used resources in the offer). If for
        whatever reason an offer is never rescinded (e.g., dropped
        message, failing over mesos_framework, etc.), a mesos_framework that attempts
        to launch tasks using an invalid offer will receive TASK_LOST
        status updates for those tasks (see Scheduler::resourceOffers).
        """

        self.logger.info("Offer rescinded %s" % (offerId.value))

    def statusUpdate(self, driver, taskStatus):
        """
        Invoked when the status of a task has changed (e.g., a slave is
        lost and so the task is lost, a task finishes and an executor
        sends a status update saying so, etc). Note that returning from
        this callback _acknowledges_ receipt of this status update! If
        for whatever reason the scheduler aborts during this callback (or
        the process exits) another status update will be delivered (note,
        however, that this is currently not true if the slave sending the
        status update is lost/fails during that time).
        """

        statuses = {
            mesos_pb2.TASK_STAGING: "STAGING",
            mesos_pb2.TASK_STARTING: "STARTING",
            mesos_pb2.TASK_RUNNING: "RUNNING",
            mesos_pb2.TASK_FINISHED: "FINISHED",
            mesos_pb2.TASK_FAILED: "FAILED",
            mesos_pb2.TASK_KILLED: "KILLED",
            mesos_pb2.TASK_LOST: "LOST",
        }

        self.logger.info("Received status update for task %s (%s)" % (
            taskStatus.task_id.value,
            statuses[taskStatus.state]
        ))

        if taskStatus.state == mesos_pb2.TASK_FINISHED or \
            taskStatus.state == mesos_pb2.TASK_FAILED or \
            taskStatus.state == mesos_pb2.TASK_KILLED or \
            taskStatus.state == mesos_pb2.TASK_LOST: \

            # Mark this task as terminal
            #self.terminal += 1
            pass

        #if self.terminal == self.total_tasks:
            #driver.stop()

    def frameworkMessage(self, driver, executorId, slaveId, data):
        """
        Invoked when an executor sends a message. These messages are best
        effort; do not expect a mesos_framework message to be retransmitted in
        any reliable fashion.
        """

        self.logger.info("Message from executor %s and slave %s: %s" % (
            executorId.value,
            slaveId.value,
            data
        ))

    def slaveLost(self, driver, slaveId):
        """
        Invoked when a slave has been determined unreachable (e.g.,
        machine failure, network partition). Most frameworks will need to
        reschedule any tasks launched on this slave on a new slave.
        """

        self.logger.info("Slave %s has been lost. Y U DO DIS." % (slaveId.value))

    def executorLost(self, driver, executorId, slaveId, exitCode):
        """
        Invoked when an executor has exited/terminated. Note that any
        tasks running will have TASK_LOST status updates automagically
        generated.
        """

        self.logger.info("Executor %s has been lost on slave %s with exit code %d" % (
            executorId.value,
            slaveId.value,
            exitCode
        ))

    def error(self, driver, message):
        """
        Invoked when there is an unrecoverable error in the scheduler or
        scheduler driver. The driver will be aborted BEFORE invoking this
        callback.
        """

        self.logger.info("There was an error: %s" % (message))

