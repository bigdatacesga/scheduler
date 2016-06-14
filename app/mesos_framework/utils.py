from __future__ import print_function

import logging
from collections import namedtuple

import requests
import registry
from ..exceptions import ResourceException

ENDPOINT = 'http://consul:8500/v1/kv'
DISKS_ENDPOINT = 'http://disks.service.int.cesga.es:5000/resources/disks/v1'

registry.connect(ENDPOINT)


def update_cluster_progress(node):
    """Update cluster launching progress"""
    cluster = node.cluster
    cluster.step += 1
    cluster.progress = float(cluster.step) / len(cluster.nodes)
    if cluster.step == len(cluster.nodes):
        cluster.status = 'launching'
    else:
        cluster.status = 'executing'


Job = namedtuple('Job', ('clusterid', 'name', 'node_dn',
                         'cpu', 'mem', 'disks', 'num_disks', 'has_custom_disks',
                         'mesos_node_hostname', 'has_custom_node',
                         'slave_id', 'offer_id'))


class JobQueue(object):
    """A job queue"""

    def __init__(self):
        self._queue = []

    def pending(self):
        """Returns the list of pending jobs"""
        return self._queue

    def append(self, nodes):
        """Adds the given node list to the to the queue"""
        for node in nodes:
            job = Job(clusterid=node.clusterid,
                      name=node.clusterid + '_' + node.name,
                      node_dn=str(node),
                      cpu=int(node.cpu), mem=int(node.mem))

            if node.custom_disks == 'True':
                disk_list = node.disks
                job.has_custom_disks = True
                disks = []
                for disk in disk_list:
                    disks.append(disk.name)
                job.disks = disks
            else:
                job.num_disks = int(node.number_of_disks)
                job.has_custom_disks = False

            if node.custom_node == 'True':
                job.mesos_node_hostname = node.mesos_node_hostname
                job.has_custom_node = True
            else:
                job.has_custom_node = False

            node.status = 'queued'

            self._queue.append(job)

    def remove(self, job):
        """Removed the given job from the queue"""
        self._queue.remove(job)


def match_mesos_node(hostname, job):
    """Verify if the hostname corresponds to the custom node requested"""
    if job.has_custom_node:
        return hostname == job.mesos_node_hostname
    return True


def match_disks(offer_disks, job):
    """Verify if the offer has the requested disks"""
    if job.has_custom_disks:
        required_disks = job.disks
        for disk in required_disks:
            if disk not in offer_disks:
                return False
    if len(offer_disks) < job.num_disks:
            return False
    return True


def get_disk_info(node, disk):
    """Get disk info from the disks service"""
    r = requests.get(DISKS_ENDPOINT + "/{}/disks/{}".format(node, disk))
    if r.status_code == 200:
        return r.json()[disk]
    else:
        raise ResourceException("Can't get disks")


def set_disk_as_used(node, node_name, disk):
    """Set the disk as used in the disks service"""
    payload = {'status': 'used', 'clustername': node_name, 'node': node}
    r = requests.put(DISKS_ENDPOINT + "/{}/disks/{}".format(node, disk), data=payload)
    if r.status_code != 204:
        raise ResourceException("Can't set disk as used")


def select_disks(job, offer_disks):
    """Select the disks to be used"""
    if job.has_custom_disks:
        required_disks = job.disks
        return required_disks

    node = registry.Node(job.node_dn)
    mesos_disks = node.disks
    used_disks = []
    for i, disk in enumerate(mesos_disks):
        used_disks.append(offer_disks[i])
        disk_mesos_name = str(offer_disks[i])
        disk.mesos_name = disk_mesos_name

        disk_info = get_disk_info(node.mesos_node_hostname, disk_mesos_name)

        disk.origin = disk_info['path'] + "/" + node.clusterid
        disk.destination = disk_info['path']
        disk.mode = disk_info['mode']

        set_disk_as_used(node.mesos_node_hostname, node.clusterid, disk_mesos_name)

    return used_disks


def remove_used_disks(offer_disks, used_disks):
    """Remove used disks from the offer disks"""
    for disk in used_disks:
        offer_disks.remove(disk)
    return offer_disks


def offer_has_enough_resources(available, job):
    """Verify if available resources satisfies the requirements of a given job"""
    if (available['cpu'] >= job.cpu and available['mem'] >= job.mem
            and available['disks'] is not None 
            and match_disks(available['disks'], job)
            and match_mesos_node(available['hostname'], job)):
        return True
    return False


def resources_from(offer):
    """Returns the available resources in the offer"""
    available = {}
    for resource in offer.resources:
        if resource.name == "cpus":
            available['cpu'] = resource.scalar.value
        if resource.name == "mem":
            available['mem'] = resource.scalar.value
        available['disks'] = None
        if resource.name == "dataDisks":
            available['disks'] = resource.set.item
    available['hostname'] = offer.hostname
