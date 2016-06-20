from __future__ import print_function

import logging
import copy

import requests
import registry
from registry import id_from


ENDPOINT = 'http://consul:8500/v1/kv'
DISKS_ENDPOINT = 'http://disks.service.int.cesga.es:5000/resources/disks/v1'

registry.connect(ENDPOINT)


class DiskServiceError(Exception):
    pass


def update_cluster_progress(node):
    """Update cluster launching progress"""
    cluster = node.cluster
    step = int(cluster.step) + 1
    total = len(cluster.nodes)
    cluster.step = step
    cluster.progress = int(float(step) / total * 100)
    if step == total:
        cluster.status = 'executing'
    else:
        cluster.status = 'launching'


class Resources(object):
    """Represents a set of resources available"""
    def __init__(self, cpus, mem, disks, host=None):
        self.cpus = cpus
        self.mem = mem
        self.disks = disks
        self.host = host


class Job(object):
    """A Job represents the resource requirements for a given cluster node

    Contains the following fields:
        cpus: number of cores
        mem: MB of memory
        disks: it can be a number or a list of specific disks
        host: if a specific docker engine host is needed
        node: the registry.Node object
    """
    def __init__(self, node):
        self.node = node
        self.name = registry.id_from(str(node))
        self.cpus = int(node.cpu)
        self.mem = int(node.mem)
        if node.required_disks == 'True':
            self.disks = [disk.name for disk in node.disks]
        else:
            self.disks = len(node.disks)

        if node.required_node == 'True':
            self.host = node.mesos_node_hostname
        else:
            self.host = None


class JobQueue(object):
    """A job queue"""

    def __init__(self):
        self._queue = []

    def pending(self):
        """Returns the list of pending jobs"""
        return copy.copy(self._queue)

    def append(self, nodes):
        """Adds the given node list to the to the queue"""
        for node in nodes:
            job = Job(node)
            #job = Job(clusterid=node.clusterid,
                      #name=node.clusterid + '_' + node.name,
                      #node_dn=str(node),
                      #cpus=int(node.cpu), mem=int(node.mem))
            node.status = 'queued'
            self._queue.append(job)

    def remove(self, job):
        """Removed the given job from the queue"""
        self._queue.remove(job)


def match_host(offered, required):
    """Verify if the hostname corresponds to the custom node requested"""
    if required:
        return offered == required
    return True


def has_enough_disks(offered, required):
    """Verify if the disks offered satisfy the requirements

       required: can be a number or a specific list of disks
    """
    # Check if specific disks are requested
    if isinstance(required, list) or isinstance(required, tuple):
        for disk in required:
            if disk not in offered:
                return False
    if len(offered) < required:
        return False
    return True


def get_disk_info(host, disk):
    """Get disk info from the disks service"""
    r = requests.get(DISKS_ENDPOINT + "/{}/disks/{}".format(host, disk))
    if r.status_code == 200:
        return r.json()[disk]
    else:
        raise DiskServiceError('Unable to get information from the disks service')


def select_disks(offered, required):
    """Select the disks to be used"""
    # If a specific list of disks is requested this are the selected disks
    if isinstance(required, list) or isinstance(required, tuple):
        return required
    # In other case just a given number of disks is requested
    selected = offered[:required]

    return selected


def update_disks_service_allocate(host, disks, nodedn):
    """Set disks as used in the disks service"""
    for disk in disks:
        set_disk_as_used(host, nodedn, disk)


def set_disk_as_used(host, nodedn, disk):
    """Set the disk as used in the disks service"""
    payload = {'status': 'used', 'clustername': nodedn, 'node': host}
    r = requests.put(DISKS_ENDPOINT + "/{}/disks/{}".format(host, disk), data=payload)
    if r.status_code != 204:
        raise DiskServiceError('Error setting disk as used in the disks service')


def update_disks_destination(disks, allocations, nodedn):
    """Update the disk.destination of each Disk object"""
    for disk, name in zip(disks, allocations):
        number = name.replace('disk', '')
        disk.destination = '/data/{}/{}'.format(number, id_from(nodedn))

def update_disks_origin(disks, allocations, nodedn):
    """Update the disk.origin of each Disk object"""
    for disk, name in zip(disks, allocations):
        number = name.replace('disk', '')
        disk.origin = '/data/{}/{}'.format(number, id_from(nodedn))


def remove_disks(offered, used):
    """Remove used disks from the offer disks"""
    for disk in used:
        offered.remove(disk)
    return offered


def offer_has_enough_resources(available, required):
    """Verify if available resources satisfies the requirements of a given job"""
    if (available.cpus >= required.cpus and available.mem >= required.mem
            and available.disks is not None
            and has_enough_disks(available.disks, required.disks)
            and match_host(available.host, required.host)):
        return True
    return False


def resources_from_offer(offer):
    """Returns the available resources in the offer"""
    for resource in offer.resources:
        if resource.name == "cpus":
            cpus = resource.scalar.value
        if resource.name == "mem":
            mem = resource.scalar.value
        disks = None
        if resource.name == "dataDisks":
            disks = resource.set.item
    host = offer.hostname
    return Resources(cpus=cpus, mem=mem, disks=disks, host=host)


def resources_from_job(job):
    """Returns the requested resources in the job"""
    return Resources(cpus=job.cpus, mem=job.mem, disks=job.disks, host=job.host)


def initialize_cluster_status(cluster):
    """Initialize cluster status"""
    cluster.status = 'queued'
    cluster.progress = 0
    cluster.step = 0
