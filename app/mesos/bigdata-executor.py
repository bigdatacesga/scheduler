#!/usr/bin/env python2.7
# A skeleton for writing a custom Mesos executor.
#
# For more information, see:
#   * https://github.com/apache/mesos/blob/0.22.2/src/python/interface/src/mesos/interface/__init__.py#L246-L310
#   * https://github.com/apache/mesos/blob/0.22.2/include/mesos/mesos.proto#L262-L291
#
from __future__ import print_function

import logging
import sys
import time
from threading import Thread
import json
import subprocess

from mesos.interface import Executor, mesos_pb2
from mesos.native import MesosExecutorDriver
import registry


class BigDataExecutor(Executor):
    def __init__(self):
        self.running_dockers = []

    def registered(self, driver, executor_info, framework_info, slave_info):
        """
          Invoked once the executor driver has been able to successfully connect
          with Mesos.  In particular, a scheduler can pass some data to its
          executors through the FrameworkInfo.ExecutorInfo's data field.
        """
        logging.info('Executor registered')

    def reregistered(self, driver, slave_info):
        """
          Invoked when the executor re-registers with a restarted slave.
        """
        pass

    def disconnected(self, driver):
        """
          Invoked when the executor becomes "disconnected" from the slave (e.g.,
          the slave is being restarted due to an upgrade).
        """
        pass

    def launchTask(self, driver, task):
        """
          Invoked when a task has been launched on this executor (initiated via
          Scheduler.launchTasks).  Note that this task can be realized with a
          thread, a process, or some simple computation, however, no other
          callbacks will be invoked on this executor until this callback has
          returned.
        """
        # Tasks should always be run in their own thread or process.
        def run_task():
            logging.info("Running task: {}".format(task.task_id.value))
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            logging.info('Received task.data: {}'.format(task.data))
            data = json.loads(task.data)
            node_dn = data['node_dn']
            logging.info('docker-executor run {}'.format(node_dn))
            self.running_dockers.append(node_dn)
            subprocess.call(['docker-executor', 'run', node_dn])

            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            driver.sendStatusUpdate(update)
            logging.info('Task finished, sent final status update.')
            self.running_dockers.remove(node_dn)

        thread = Thread(target=run_task, args=())
        thread.start()

    def killTask(self, driver, task_id):
        """
          Invoked when a task running within this executor has been killed (via
          SchedulerDriver.killTask).  Note that no status update will be sent on
          behalf of the executor, the executor is responsible for creating a new
          TaskStatus (i.e., with TASK_KILLED) and invoking ExecutorDriver's
          sendStatusUpdate.
        """
        logging.info('Killing task {}'.format(task_id))
        node_dn = registry.dn_from(task_id)
        logging.info('docker-executor destroy {}'.format(node_dn))
        subprocess.call(['docker-executor', 'destroy', node_dn])
        self.running_dockers.remove(node_dn)

    def frameworkMessage(self, driver, message):
        """
          Invoked when a framework message has arrived for this executor.  These
          messages are best effort; do not expect a framework message to be
          retransmitted in any reliable fashion.
        """
        pass

    def shutdown(self, driver):
        """
          Invoked when the executor should terminate all of its currently
          running tasks.  Note that after Mesos has determined that an executor
          has terminated any tasks that the executor did not send terminal
          status updates for (e.g., TASK_KILLED, TASK_FINISHED, TASK_FAILED,
          etc) a TASK_LOST status update will be created.
        """
        for node_dn in self.running_dockers:
            subprocess.call(['docker-executor', 'destroy', node_dn])

    def error(self, error, message):
        """
          Invoked when a fatal error has occured with the executor and/or
          executor driver.  The driver will be aborted BEFORE invoking this
          callback.
        """
        logging.error(message)


def main():
    driver = MesosExecutorDriver(BigDataExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)


if __name__ == '__main__':
    main()
