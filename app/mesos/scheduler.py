#!/usr/bin/env python2.7
"""Big Data PaaS scheduler

For more information, see:
 - https://github.com/apache/mesos/blob/0.22.2/src/python/interface/src/mesos/interface/__init__.py#L34-L129
 - https://github.com/apache/mesos/blob/0.22.2/include/mesos/mesos.proto

Based on the skeleton provided in chapter 10 of mesos in action:

  - https://github.com/rji/mesos-in-action-code-samples

"""
from __future__ import print_function


import logging
import os
import signal
import sys
import time
import uuid
from threading import Thread
import json

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver

import requests
import registry
from . import utils
from ..exceptions import ResourceException

logger = logging.getLogger(__name__)

ENDPOINT = 'http://consul:8500/v1/kv'
DISKS_ENDPOINT = 'http://disks.service.int.cesga.es:5000/resources/disks/v1'


class BigDataScheduler(Scheduler):
    def __init__(self, executor):
        self.executor = executor
        self.queue = utils.JobQueue()
        registry.connect(ENDPOINT)

    def registered(self, driver, framework_id, master_info):
        """
          Invoked when the scheduler successfully registers with a Mesos master.
          It is called with the frameworkId, a unique ID generated by the
          master, and the masterInfo which is information about the master
          itself.
        """
        logging.info("Registered with framework ID: {}".format(framework_id.value))

    def reregistered(self):
        """
          Invoked when the scheduler re-registers with a newly elected Mesos
          master.  This is only called when the scheduler has previously been
          registered.  masterInfo contains information about the newly elected
          master.
        """
        logging.info('Reregistered')

    def disconnected(self):
        """
          Invoked when the scheduler becomes disconnected from the master, e.g.
          the master fails and another is taking over.
        """
        logging.info('Disconnected')

    def resourceOffers(self, driver, offers):
        """
          Invoked when resources have been offered to this framework. A single
          offer will only contain resources from a single slave.  Resources
          associated with an offer will not be re-offered to _this_ framework
          until either (a) this framework has rejected those resources (see
          SchedulerDriver.launchTasks) or (b) those resources have been
          rescinded (see Scheduler.offerRescinded).  Note that resources may be
          concurrently offered to more than one framework at a time (depending
          on the allocator being used).  In that case, the first framework to
          launch tasks using those resources will be able to use them while the
          other frameworks will have those resources rescinded (or if a
          framework has already launched tasks with those resources then those
          tasks will fail with a TASK_LOST status and a message saying as much).
        """
        for offer in offers:
            logger.debug('Received offer with ID: {}'.format(offer.id.value))
            available = utils.resources_from_offer(offer)
            logger.debug('Resources offered: node={}, cpus={}, mem={}, disks={}'
                         .format(offer.hostname, available.cpus, available.mem,
                                 available.disks))
            # Mesos tasks to launch generated from the job queue
            tasks = []
            for job in self.queue.pending():
                required = utils.resources_from_job(job)
                if utils.offer_has_enough_resources(available, required):
                    logger.info('OfferID {} resources: node={}, cpus={}, mem={}, disks={}'
                                .format(offer.id.value, offer.hostname,
                                        available.cpus, available.mem, available.disks))
                    logger.info('Job {} fits into offer'.format(job.name))
                    allocated_disks = utils.select_disks(available.disks, required.disks)
                    # TODO: Refactor disks allocation to a method
                    try:
                        utils.update_disks_service_allocate(
                            offer.hostname, allocated_disks, str(job.node))
                    except(ResourceException):
                        logger.error("Task %s encountered resource error with Offer %s "
                                     "in node %s", job.name, offer.id, offer.hostname)
                        logger.error("Please check that a node with \"%s\" name exists "
                                     "in the resource tree of the kvstore", offer.hostname)
                        driver.declineOffer(offer.id)
                        #FIXME Offer can't be declined if it was previously used
                        break
                    job.disks = allocated_disks
                    logger.info('Disks allocated for this job: {}'.format(job.disks))
                    job.slave_id = offer.slave_id.value
                    job.hostname = offer.hostname
                    job.offer_id = offer.id

                    # Update node object information
                    node = job.node
                    node.mesos_slave_id = offer.slave_id.value
                    node.mesos_node_hostname = offer.hostname
                    node.mesos_offer_id = offer.id
                    utils.update_disks_origin(node.disks, allocated_disks, str(node))
                    # FIXME wasn't this supposed to be done in the docker-executor
                    utils.update_disks_destination(node.disks, allocated_disks, str(node))

                    # Reduce the remaining available resourses from this offer
                    available.cpus -= job.cpus
                    available.mem -= job.mem
                    available.disks = utils.remove_disks(available.disks, allocated_disks)
                    logger.info('Remaining offer resources: cpus={}, mem={}, disks={}'
                                .format(available.cpus, available.mem, available.disks))

                    utils.update_cluster_progress(node)

                    logger.info("Scheduling new task for launch: {}".format(job.name))
                    tasks.append(self.task_from(job))
                    self.queue.remove(job)
            if tasks:
                logger.info('Launching all tasks that fit inside this offer: {}'
                            .format([t.name for t in tasks]))
                logger.debug('Task details: \n{}'.format(tasks))
                driver.launchTasks(offer.id, tasks)
            else:
                driver.declineOffer(offer.id)

    def offerRescinded(self, driver, offer_id):
        """
          Invoked when an offer is no longer valid (e.g., the slave was lost or
          another framework used resources in the offer.) If for whatever reason
          an offer is never rescinded (e.g., dropped message, failing over
          framework, etc.), a framwork that attempts to launch tasks using an
          invalid offer will receive TASK_LOST status updats for those tasks
          (see Scheduler.resourceOffers).
        """
        pass

    def statusUpdate(self, driver, update):
        """
          Invoked when the status of a task has changed (e.g., a slave is
          lost and so the task is lost, a task finishes and an executor
          sends a status update saying so, etc). If implicit
          acknowledgements are being used, then returning from this
          callback _acknowledges_ receipt of this status update! If for
          whatever reason the scheduler aborts during this callback (or
          the process exits) another status update will be delivered (note,
          however, that this is currently not true if the slave sending the
          status update is lost/fails during that time). If explicit
          acknowledgements are in use, the scheduler must acknowledge this
          status on the driver.
        """
        logger.info("Task {} is in state {}".format(
            update.task_id.value, mesos_pb2.TaskState.Name(update.state)))

    def frameworkMessage(self, driver, executor_id, slave_id, message):
        """
          Invoked when an executor sends a message. These messages are best
          effort; do not expect a framework message to be retransmitted in any
          reliable fashion.
        """
        pass

    def slaveLost(self, driver, slave_id):
        """
          Invoked when a slave has been determined unreachable (e.g., machine
          failure, network partition.) Most frameworks will need to reschedule
          any tasks launched on this slave on a new slave.
        """
        pass

    def executorLost(self, driver, executor_id, slave_id, status):
        """
          Invoked when an executor has exited/terminated. Note that any tasks
          running will have TASK_LOST status updates automatically generated.
        """
        pass

    def error(self, driver, message):
        """
          Invoked when there is an unrecoverable error in the scheduler or
          scheduler driver.  The driver will be aborted BEFORE invoking this
          callback.
        """
        logger.error(message)

    def task_from(self, job):
        """Create a mesos task from a given job"""
        task = mesos_pb2.TaskInfo()
        #task_id = str(uuid.uuid4())
        task.task_id.value = job.name
        task.slave_id.value = job.slave_id
        task.name = job.name
        task.data = json.dumps({"node_dn": str(job.node)})
        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = job.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = job.mem

        disks = task.resources.add()
        disks.name = "dataDisks"
        disks.type = mesos_pb2.Value.SET

        required_tasks_disks = job.disks
        used_disks = required_tasks_disks
        for disk in used_disks:
            disks.set.item.append(disk)

        return task

    def enqueue(self, cluster):
        """Enqueue all nodes of a given cluster"""
        utils.initialize_cluster_status(cluster)
        self.queue.append(cluster.nodes)

    def pending(self):
        """Returns the list of pending jobs"""
        return self.queue.pending()
