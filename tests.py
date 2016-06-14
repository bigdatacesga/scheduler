"""Tests for mesos scheduler"""
import unittest
import registry
from app.mesos_framework import utils

ENDPOINT = 'http://10.112.0.101:8500/v1/kv'


class StatusTestCase(unittest.TestCase):

    def setUp(self):
        registry.connect(ENDPOINT)

    def tearDown(self):
        pass

    def test_update_cluster_progress(self):
        node = registry.Node('__tests__/cluster1/nodes/node1')
        cluster = node.cluster
        cluster.progress = 1
        utils.update_cluster_progress(node)
        self.assertEqual(cluster.progress, 2)
