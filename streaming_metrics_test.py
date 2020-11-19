import urllib.request
import pytest
import logging

from cassandra import ConsistencyLevel
from tools.jmxutils import (JolokiaAgent, make_mbean)
from dtest import Tester, create_ks, create_cf
from tools.data import insert_c1c2


class TestStreamingMetrics(Tester):

    def test_streaming_metrics(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        nodes = cluster.nodelist()

        for i in range(0, len(nodes)):
            nodes[i].set_configuration_options(values={'initial_token': tokens[i]})

        cluster.start()

        session = self.patient_cql_connection(nodes[0])

        create_ks(session, name='ks2', rf=2)

        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'},
                        compaction_strategy='LeveledCompactionStrategy')
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)

        session_n2 = self.patient_exclusive_cql_connection(nodes[1])
        session_n2.execute("TRUNCATE system.available_ranges;")

        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':3};")
        nodes[0].nodetool('repair -full ks2 cf')

        with JolokiaAgent(nodes[0]) as jmx:

            incoming_bytes = make_mbean('metrics', type='Streaming', scope='/127.0.0.2.7000', name = 'IncomingBytes')
            incoming_bytes_count = jmx.read_attribute(incoming_bytes, 'Count')

            outgoing_bytes = make_mbean('metrics', type='Streaming', scope='/127.0.0.2.7000', name = 'OutgoingBytes')
            outgoing_bytes_count = jmx.read_attribute(outgoing_bytes, 'Count')

            total_incoming_bytes = make_mbean('metrics', type='Streaming', name = 'TotalIncomingBytes')
            total_incoming_bytes_count = jmx.read_attribute(total_incoming_bytes, 'Count')

            total_outgoing_bytes = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingBytes')
            total_outgoing_bytes_count = jmx.read_attribute(total_outgoing_bytes, 'Count')

            total_outgoing_repair_bytes = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingRepairBytes')
            total_outgoing_repair_bytes_count = jmx.read_attribute(total_outgoing_repair_bytes, 'Count')

            total_outgoing_repair_sstables = make_mbean('metrics', type='Streaming', name = 'TotalOutgoingRepairSSTables')
            total_outgoing_repair_sstables_count = jmx.read_attribute(total_outgoing_repair_sstables, 'Count')

        assert incoming_bytes_count > 0
        assert outgoing_bytes_count > 0
        assert total_incoming_bytes_count > 0
        assert total_outgoing_bytes_count > 0
        assert total_outgoing_repair_bytes_count > 0
        assert total_outgoing_repair_sstables_count > 0