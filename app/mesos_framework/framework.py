import logging
import mesos.interface
import mesos.native
import threading
from mesos.interface import mesos_pb2
from . import scheduler


class MesosFramework:

    class __MesosFramework:
        def __init__(self, arg):
            self.val = arg

        def __str__(self):
            return repr(self) + self.val
    instance = None

    def __init__(self, arg):
        if not MesosFramework.instance:
            self.mesosDockerframework = self.get_mesos_docker_framework()
            self.mesosScheduler = self.get_mesos_scheduler()
            self.mesosMasterAddress = 'mesosmaster.service.int.cesga.es:5050'
            self.start_mesos_driver()
            MesosFramework.instance = MesosFramework.__MesosFramework(arg)
        else:
            MesosFramework.instance.val = arg

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def get_mesos_scheduler(self):

        # Setup the loggers
        logger = logging.getLogger('mesos')
        logger.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        res = logging.StreamHandler()
        res.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        res.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(res)

        implicitAcknowledgements = 1
        mesosScheduler = scheduler.MyMesosScheduler(implicitAcknowledgements)

        return mesosScheduler

    def get_mesos_driver(self, mesosScheduler, framework, mesosMasterIpAndPort):
        driver = mesos.native.MesosSchedulerDriver(
            mesosScheduler,
            framework,
            mesosMasterIpAndPort)
        return driver

    def get_mesos_docker_framework(self):
        # Launch the mesos mesos_framework
        framework = mesos_pb2.FrameworkInfo()
        framework.user = "root"  # Or leave empty to have Mesos fill in the current user.
        framework.name = "BigDataServices"
        framework.principal = "BigDataServices_MesosFramework"
        return framework

    def start_mesos_driver(self):
        driver = self.get_mesos_driver(self.mesosScheduler, self.mesosDockerframework, self.mesosMasterAddress)
        t = threading.Thread(target=driver.run)
        t.setDaemon(True)
        t.start()

    def add_task_to_queue(self, instance_path):
        self.mesosScheduler.queue_new_instance(instance_path)

    def get_queued_instances(self):
        return self.mesosScheduler.get_queued_instances()
