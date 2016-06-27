"""Tests for mesos scheduler"""
import unittest
import registry
import utils
from mesos.interface import mesos_pb2
import uuid

ENDPOINT = 'http://10.112.0.101:8500/v1/kv'


class MockDisk(object):
    def __init__(self):
        self.origin = None


class StatusTestCase(unittest.TestCase):

    def setUp(self):
        registry.connect(ENDPOINT)
        # FIXME: Instantiating/deinstantiating each time is too slow
        self.cluster = registry.instantiate('test', 'example', '0.1.0', {'size': 2})
        self.node = self.cluster.nodes[0]
        #self.cluster = registry.Cluster('instances/test/example/0.1.0/1')
        #self.node = registry.Node('instances/test/example/0.1.0/1/nodes/example1')

    def tearDown(self):
        # FIXME: Instantiating/deinstatiating each time is too slow
        dn = self.cluster.dn
        id = dn[dn.rfind('/')+1:]
        registry.deinstantiate('test', 'example', '0.1.0', id)

    def test_update_cluster_step(self):
        node = self.node
        cluster = node.cluster
        cluster.step = 0
        utils.update_cluster_progress(node)
        self.assertEqual(cluster.step, '1')

    def test_update_cluster_progress_launching(self):
        node = self.node
        cluster = node.cluster
        cluster.status = 'launching'
        cluster.step = 0
        cluster.progress = 0
        utils.update_cluster_progress(node)
        self.assertEqual(cluster.progress, '50')
        self.assertEqual(cluster.status, 'launching')

    def test_update_cluster_progress_executing(self):
        node = self.node
        cluster = node.cluster
        cluster.status = 'launching'
        cluster.step = 1
        cluster.progress = 50
        utils.update_cluster_progress(node)
        self.assertEqual(cluster.progress, '100')
        self.assertEqual(cluster.status, 'executing')

    def test_obtain_resources_from_offer(self):
        offer = self.generate_offer(cpus=12, mem=8096, disks=('disk1', 'disk2'))
        resources = utils.resources_from_offer(offer)
        self.assertEqual(resources.cpus, 12)
        self.assertEqual(resources.mem, 8096)
        self.assertEqual(resources.disks, ['disk1', 'disk2'])

    def test_offer_has_enough_resources(self):
        offered = utils.Resources(cpus=2, mem=1024, disks=['disk1', 'disk2'])
        required = utils.Resources(cpus=2, mem=1024, disks=2)
        self.assertTrue(utils.offer_has_enough_resources(offered, required))

    def test_has_enough_disks_without_custom_disks(self):
        offered = ['disk1', 'disk2']
        required = 2
        self.assertTrue(utils.has_enough_disks(offered, required))

    def test_has_enough_disks_with_custom_disks_does_not_match(self):
        offered = ['disk1', 'disk2']
        required = ['disk8', 'disk9']
        self.assertFalse(utils.has_enough_disks(offered, required))

    def test_has_enough_disks_with_custom_disks_matches(self):
        offered = ['disk1', 'disk2']
        required = ['disk8', 'disk9']
        self.assertFalse(utils.has_enough_disks(offered, required))

    def test_match_host(self):
        offered = 'c14-1'
        required = 'c14-1'
        self.assertTrue(utils.match_host(offered, required))

    def test_match_host_without_fixed_node(self):
        offered = 'c14-1'
        required = None
        self.assertTrue(utils.match_host(offered, required))

    def test_match_host_requested_host_not_available(self):
        offered = 'c14-1'
        required = 'c13-9'
        self.assertFalse(utils.match_host(offered, required))

    def test_remove_disks(self):
        offered = ['disk1', 'disk2', 'disk5', 'disk7']
        used = ['disk1', 'disk5']
        expected = ['disk2', 'disk7']
        self.assertEqual(utils.remove_disks(offered, used), expected)

    def test_select_disks_with_specific_disks(self):
        offered = ['disk1', 'disk2', 'disk5', 'disk7']
        required = ['disk1', 'disk5']
        selected = utils.select_disks(offered, required)
        expected = ['disk1', 'disk5']
        self.assertEqual(selected, expected)

    def test_select_disks_just_a_number(self):
        offered = ['disk1', 'disk2', 'disk5', 'disk7']
        required = 2
        selected = utils.select_disks(offered, required)
        expected = ['disk1', 'disk2']
        self.assertEqual(selected, expected)

    def test_get_disk_info(self):
        disk = utils.get_disk_info('c13-1', 'disk1')
        self.assertTrue('path' in disk)

    def test_update_disks_origin(self):
        disks = []
        disk1 = MockDisk()
        disks.append(disk1)
        disk2 = MockDisk()
        disks.append(disk2)
        allocations = ['disk1', 'disk4']
        nodedn = str(self.node)
        utils.update_disks_origin(disks, allocations, nodedn)
        expected = '/data/1/{}'.format(registry.id_from(nodedn))
        self.assertEqual(disks[0].origin, expected)
        expected = '/data/4/{}'.format(registry.id_from(nodedn))
        self.assertEqual(disks[1].origin, expected)

    def test_update_disks_service_allocate(self):
        raise NotImplementedError

    def test_set_disk_as_used(self):
        raise NotImplementedError

    def test_initialize_cluster_status(self):
        cluster = self.cluster
        cluster.status = 'pending'
        utils.initialize_cluster_status(cluster)
        self.assertEqual(cluster.status, 'queued')
        self.assertEqual(cluster.progress, '0')

    def generate_offer(self, cpus=1, mem=1024, disks=('disk1')):
        offer = mesos_pb2.Offer()
        offer.id.value = str(uuid.uuid4())
        offer.framework_id.value = 'PaaS'
        offer.slave_id.value = 'c14-5'
        offer.hostname = 'c14-5'

        offer_cpus = offer.resources.add()
        offer_cpus.name = "cpus"
        offer_cpus.type = mesos_pb2.Value.SCALAR
        offer_cpus.scalar.value = 12

        offer_mem = offer.resources.add()
        offer_mem.name = "mem"
        offer_mem.type = mesos_pb2.Value.SCALAR
        offer_mem.scalar.value = 8096

        offer_disks = offer.resources.add()
        offer_disks.name = "dataDisks"
        offer_disks.type = mesos_pb2.Value.SET
        for disk in disks:
            offer_disks.set.item.append(disk)

        return offer


if __name__ == '__main__':
    unittest.main()
