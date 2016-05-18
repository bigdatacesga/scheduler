from mesos.interface import mesos_pb2

import registry

# Create a global kvstore client
ENDPOINT = 'http://10.112.0.101:8500/v1/kv'

class MyMesosLauncher():

    def get_instance_info(self):
        pass

    def launch_nodes(self, tasks, driver, offerId):
        registry.connect(ENDPOINT)
        task_templates = []
        for task in tasks:
            node = registry.Node(task["node_dn"])
            node.status = "launching"

            disks = []
            for disk in node.disks:
                disks.append(disk.mesos_name)

            task_data = dict()
            task_data["cpu"] = int(node.cpu)
            task_data["mem"] = int(node.mem)
            task_data["name"] = node.name
            task_data["node_id"] = node.node_id
            task_data["disks"] = disks
            task_data["mem"] = int(node.mem)
            task_data["instance_dn"] = str(node)

            task_template = self.new_instance(node.mesos_slave_id, task_data)
            task_templates.append(task_template)

        driver.launchTasks(offerId, task_templates)

    def new_instance(self, slave_id, task_data):
        '''
        Creates a task for mesos
        :param slave_id: mesos slave id
        :type slave_id: ?
        :param task_data: task parameters for resources needs
        :type id: dict
        '''
        task = mesos_pb2.TaskInfo()

        task.task_id.value = str(task_data["node_id"])
        task.slave_id.value = slave_id
        task.name = task_data["name"]

        # Define the command line to execute in the Docker container
        # command = mesos_pb2.CommandInfo()
        # command.shell = False
        # command.value = "service sshd start && while true; do sleep 3600; done"
        # command.value = "echo \"TESTING\" && sleep 60"
        # command.value = ""

        task.command.value = "docker-executor run " + task_data["instance_dn"]

        #executor = mesos_pb2.ExecutorInfo()
        #executor.executor_id.value = "cesga_docker_executor_{}".format(str(task_data["node_id"]))
        #executor.name = "My docker executor"
        #executor.command.value = "docker-executor run " + task_data["instance_dn"]
        #task.executor.MergeFrom(executor)

        # CPUs are repeated elements too
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = task_data["cpu"]

        # Memory are repeated elements too
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = task_data["mem"]

        # Disks
        disks = task.resources.add()
        disks.name = "dataDisks"
        disks.type = mesos_pb2.Value.SET

        required_tasks_disks = task_data["disks"]
        used_disks = required_tasks_disks
        for disk in used_disks:
            disks.set.item.append(disk)

        # Return the object and the disks used
        return task