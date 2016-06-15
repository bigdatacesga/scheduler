"""Big Data Mesos Framework"""
import logging
import os
import signal
import sys
import time
from threading import Thread

from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver
from scheduler import BigDataScheduler

driver = None
scheduler = None


def submit(cluster):
    """Submit a cluster instance to Mesos"""
    scheduler.enqueue(cluster)


def kill(cluster):
    taskid = mesos_pb2.TaskID()
    taskid.value = str(cluster).replace("/", "_").replace(".", "-")
    driver.killTask(taskid)

    # service = registry.Cluster(instance_id)
    # nodesList = service.nodes
    # for node in nodesList:
    #     clusterid = node.clusterid + "_" + node.name
    #     message = TaskID()
    #     message.value = clusterid
    #     self.driver.killTask(message)


def pending(self):
    return scheduler.pending()


def start(master):
    """Start the Big Data framework"""
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s %(levelname)s] %(message)s')

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = 'BigDataExecutor'
    executor.name = executor.executor_id.value
    executor.command.value = '/usr/local/mesos/bin/paas-executor.py'

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ''  # the current user
    framework.name = 'PaaS'
    framework.checkpoint = True

    scheduler = BigDataScheduler(executor)

    implicitAcknowledgements = 1

    if os.getenv('MESOS_AUTHENTICATE'):
        logging.info('Enabling framework authentication')

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv('MESOS_PRINCIPAL')
        credential.secret = os.getenv('MESOS_SECRET')
        framework.principal = os.getenv('MESOS_PRINCIPAL')

        driver = MesosSchedulerDriver(scheduler, framework, master,
                                      implicitAcknowledgements, credential)
    else:
        framework.principal = framework.name
        driver = MesosSchedulerDriver(scheduler, framework, master, implicitAcknowledgements)

    def signal_handler(signal, frame):
        logging.info('Shutting down')
        driver.stop()

    # driver.run() blocks, so we run it in a separate thread.
    # This way, we can catch a SIGINT to kill the framework.
    def run_driver_thread():
        status = 0 if driver.run() == mesos_pb2.driver_STOPPED else 1
        driver.stop()  # Ensure the driver process terminates
        sys.exit(status)

    driver_thread = Thread(target=run_driver_thread, args=())
    driver_thread.start()

    logging.info('Scheduler running, Ctrl-C to exit')
    signal.signal(signal.SIGINT, signal_handler)

    # Block the main thread while the driver thread is alive
    while driver_thread.is_alive():
        time.sleep(1)

    logging.info('Framework finished.')
    sys.exit(0)
