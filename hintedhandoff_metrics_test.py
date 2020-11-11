import os
import time
import pytest
import logging
import yaml

from cassandra import ConsistencyLevel

from dtest import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.assertions import assert_stderr_clean
from tools.jmxutils import (JolokiaAgent, enable_jmx_ssl, make_mbean)

since = pytest.mark.since
logger = logging.getLogger(__name__)

class TestHintedHandOffMetrics(Tester):
    """
    Testing metrics: (hintsSucceeded, hintsFailed, hintsTimedOut, globalDelayHistogram, delayByEndpoint) for hinted handoffs

    @jira_ticket CASSANDRA-16189
    """

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if not self.dtest_config.use_vnodes:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        assert_stderr_clean(err)
        return out

    def _do_hinted_handoff(self, node1, node2, enabled, keyspace='ks'):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, keyspace, 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start()

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace=keyspace)
        for n in range(0, 100):
            if enabled:
                query_c1c2(session, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(session, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    def _drop_node2_and_do_hh(self, node1, node2, enabled, keyspace='ks'):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, keyspace, 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start()

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=60)
        # node1.stop(wait_other_notice=True)

    
    def test_hintedhandoff_enabled(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, True)
    
    def test_hintedhandoff_metrics(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        node2_host_address = node2.address()
        # node2_jmx_port = node2.jmx_port
        node2_jmx_port = '7000' # assuming that jmx port is 7000, as above statement returns 7200.

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._drop_node2_and_do_hh(node1, node2, True)

        # time.sleep(60)
        metrics_dict = {}
        node_suffix = node2_host_address+'.'+node2_jmx_port
        hints_created_name_for_node2 = 'Hints_created-/'+node_suffix
        hints_delayed_name_for_node2 = 'Hint_delays-/'+node_suffix

        with JolokiaAgent(node1) as jmx:
            for metric in ['HintsSucceeded', 'HintsFailed', 'HintsTimedOut']:
                metrics_dict[metric] = {}
                for attribute in ['Count', 'FifteenMinuteRate', 'FiveMinuteRate', 'MeanRate', 'OneMinuteRate', 'RateUnit']:
                    mbean = make_mbean('metrics', type='HintsService', name = metric)
                    metrics_string = jmx.read_attribute(mbean, attribute)
                    metrics_dict[metric][attribute] = metrics_string
            
            for metric in [hints_created_name_for_node2, 'Hint_delays', hints_delayed_name_for_node2]:
                metrics_dict[metric] = {}
                attribute = 'Count'
                mbean = make_mbean('metrics', type='HintsService', name = metric)
                metrics_string = jmx.read_attribute(mbean, attribute)
                metrics_dict[metric][attribute] = metrics_string
        
        with open('hintedhandoff_metrics_test_log.txt','w') as fl:
            yaml.dump(metrics_dict, fl, default_flow_style=False)
        
        assert metrics_dict['HintsFailed']['RateUnit'] == 'events/second'
        assert metrics_dict['HintsTimedOut']['RateUnit'] == 'events/second'
        assert metrics_dict['HintsSucceeded']['RateUnit'] == 'events/second'

        # add this line if you think metrics should be created.
        assert metrics_dict[hints_created_name_for_node2]['Count'] > 0
        
        assert metrics_dict[hints_delayed_name_for_node2]['Count'] >= 0
        assert metrics_dict['Hint_delays']['Count'] >= metrics_dict[hints_delayed_name_for_node2]['Count']

        if metrics_dict[hints_created_name_for_node2]['Count'] > 0:
            if metrics_dict['HintsFailed']['Count'] > 0:
                if metrics_dict['HintsTimedOut']['Count'] > 0:
                    assert (metrics_dict['HintsTimedOut']['Count'] > 0 and metrics_dict['HintsTimedOut']['Count'] <= metrics_dict['HintsFailed']['Count'])

                if metrics_dict['HintsSucceeded']['Count'] > 0:
                    assert metrics_dict['HintsSucceeded']['Count'] == metrics_dict[hints_created_name_for_node2]['Count'] - metrics_dict['HintsFailed']['Count']

                assert metrics_dict['HintsSucceeded']['Count'] == metrics_dict[hints_created_name_for_node2]['Count']

            else:
                assert metrics_dict['HintsSucceeded']['Count'] == metrics_dict[hints_created_name_for_node2]['Count'] - metrics_dict['HintsFailed']['Count']

        else:
            assert metrics_dict['HintsFailed']['Count'] == 0
            assert metrics_dict['HintsTimedOut']['Count'] == 0
            assert metrics_dict['HintsSucceeded']['Count'] == 0