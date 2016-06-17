"""Big Data Mesos Framework"""
import logging
import os
from threading import Thread

from mesos.interface import mesos_pb2
from mesos.native import MesosSchedulerDriver
from scheduler import BigDataScheduler
import registry

logger = logging.getLogger(__name__)

driver = None
scheduler = None
STARTED = False


def submit(cluster):
    """Submit a cluster instance to Mesos"""
    scheduler.enqueue(cluster)


def kill(cluster):
    """Kill all the tasks of a given cluster"""
    for node in cluster.nodes:
        taskid = mesos_pb2.TaskID()
        taskid.value = registry.id_from(str(node))
        logger.info('Killing taskid {}'.format(taskid.value))
        driver.killTask(taskid)


def pending():
    """Return the list of pending tasks"""
    return scheduler.pending()


def start(master):
    """Start the Big Data framework"""
    global driver, scheduler

    if driver and scheduler:
        return driver, scheduler

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = 'BigDataExecutor'
    executor.name = executor.executor_id.value
    executor.command.value = '/usr/local/mesos/bin/bigdata-executor.py'

    framework = mesos_pb2.FrameworkInfo()
    # Leave empty to have Mesos fill in the current user
    framework.user = 'root'
    framework.name = 'PaaS'
    framework.checkpoint = True

    scheduler = BigDataScheduler(executor)

    implicitAcknowledgements = 1

    if os.getenv('MESOS_AUTHENTICATE'):
        logger.info('Enabling framework authentication')

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv('MESOS_PRINCIPAL')
        credential.secret = os.getenv('MESOS_SECRET')
        framework.principal = os.getenv('MESOS_PRINCIPAL')

        driver = MesosSchedulerDriver(scheduler, framework, master,
                                      implicitAcknowledgements, credential)
    else:
        framework.principal = framework.name
        driver = MesosSchedulerDriver(scheduler, framework, master, implicitAcknowledgements)

    # driver.run() blocks, so we run it in a separate thread.
    # This way, we can catch a SIGINT to kill the framework.
    def run_driver_thread():
        status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
        return status

    driver_thread = Thread(target=run_driver_thread, args=())
    # Stop abruptly the thread if the main process exits
    #driver_thread.setDaemon(True)
    driver_thread.start()

    logger.info('Scheduler running')

    return driver, scheduler


def stop():
    """Stop the framework"""
    global driver, scheduler
    logger.info('Shutting down scheduler')
    driver.stop()
    driver = None
    scheduler = None
