# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import json
import re
import subprocess
import time

from ducktape.mark import ignore
from ducktape.tests.test import TestLoggerMaker
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, RedpandaService, SecurityConfig
from rptest.services.rpk_producer import RpkProducer
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kcl import KCL


def get_topics_and_partitions(reassignments: dict):
    topic_names = []
    partition_idxs = []
    for reassignment in reassignments["partitions"]:
        topic_names.append(reassignment["topic"])
        partition_idxs.append(reassignment["partition"])

    return topic_names, partition_idxs


def check_execute_reassign_partitions(lines: list[str], reassignments: dict,
                                      logger: TestLoggerMaker):
    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Output has the following structure
    # Line 0 - "Current partition replica assignment"
    # Line 1 - empty
    # Line 2 - Json structure showing the current replica sets for each partition
    # Line 3 - empty
    # Line 4 - "Save this to use as the --reassignment-json-file option during rollback"
    # Line 5 - "Successfully started partition reassignments for topic1-p0,topic1-p1,...,topic2-p0,topic2-p1,..."

    # The last line should list topic partitions
    line = lines.pop().strip()
    topic_partitions = None
    if len(partition_idxs) > 1:
        assert line.startswith(
            "Successfully started partition reassignments for ")
        topic_partitions = line.removeprefix(
            "Successfully started partition reassignments for ").split(',')
    else:
        assert line.startswith(
            "Successfully started partition reassignment for ")
        topic_partitions = line.removeprefix(
            "Successfully started partition reassignment for ").split(',')

    tp_re = re.compile(r"^(?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?)$")
    for tp in topic_partitions:
        tp_match = tp_re.match(tp)
        logger.debug(f"topic partition match: {tp}, {tp_match}")
        assert tp_match is not None
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs

    # The next lines are exact strings
    assert lines.pop().strip(
    ) == "Save this to use as the --reassignment-json-file option during rollback"
    assert len(lines.pop()) == 0

    # Then a json structure
    current_assignment = json.loads(lines.pop().strip())
    assert type(current_assignment) == type({}), "Expected JSON object"

    # Then another exact string
    assert len(lines.pop()) == 0
    assert lines.pop().strip() == "Current partition replica assignment"

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def check_verify_reassign_partitions(lines: list[str], reassignments: dict,
                                     rp_node_idxs: list[str],
                                     logger: TestLoggerMaker):

    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Check output
    # Output has the following structure
    # Line 0 - "Status of partition reassignment:"
    #        - One line for each topic partition
    #        - Empty string
    #        - "Clearing broker-level throttles on brokers node_id1,node_id2,..."
    #        - An InvalidConfigurationException because the kafka script attempts to alter broker configs
    #          on a per-node basis which Redpanda does not support

    lines.reverse()

    # First line is an exact string
    assert lines.pop().strip() == "Status of partition reassignment:"

    # Then there is one line for each topic partition
    tp_re_complete = re.compile(
        r"^Reassignment of partition (?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?) is complete.$"
    )
    tp_re_no_active = re.compile(
        r"^There is no active reassignment of partition (?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?), but replica set is.*$"
    )

    def re_match(line):
        m = tp_re_complete.match(line)
        if m is not None:
            return m

        return tp_re_no_active.match(line)

    line = lines.pop().strip()
    tp_match = re_match(line)
    logger.debug(f"topic partition match: {line} {tp_match}")
    while tp_match is not None:
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs
        line = lines.pop().strip()
        tp_match = re_match(line)
        logger.debug(f"topic partition match: {line} {tp_match}")

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def check_cancel_reassign_partitions(lines: list[str], reassignments: dict,
                                     logger: TestLoggerMaker):
    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Output has the following structure
    # Line 0 - "Successfully cancelled partition reassignments for: topic1-p0,topic1-p1,...,topic2-p0,topic2-p1,..."
    # Line 1 - "None of the specified partition moves are active."

    lines.reverse()

    # The last line should list topic partitions
    line = lines.pop().strip()
    topic_partitions = None
    if len(partition_idxs) > 1:
        assert line.startswith(
            "Successfully cancelled partition reassignments for: ")
        topic_partitions = line.removeprefix(
            "Successfully cancelled partition reassignments for: ").split(',')
    else:
        assert line.startswith(
            "Successfully cancelled partition reassignment for: ")
        topic_partitions = line.removeprefix(
            "Successfully cancelled partition reassignment for: ").split(',')

    tp_re = re.compile(r"^(?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?)$")
    for tp in topic_partitions:
        tp_match = tp_re.match(tp)
        logger.debug(f"topic partition match: {tp}, {tp_match}")
        assert tp_match is not None
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs

    # The next lines are exact strings
    assert lines.pop().strip(
    ) == "None of the specified partition moves are active."

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


log_config = LoggingConfig('info',
                           logger_levels={
                               'kafka': 'trace',
                               'kafka/client': 'trace',
                               'cluster': 'trace',
                               'raft': 'trace'
                           })


class PartitionReassignmentsTest(RedpandaTest):
    REPLICAS_COUNT = 3
    PARTITION_COUNT = 3
    topics = [
        TopicSpec(partition_count=PARTITION_COUNT),
        TopicSpec(partition_count=PARTITION_COUNT),
    ]

    def __init__(self, test_context):
        super(PartitionReassignmentsTest,
              self).__init__(test_context=test_context,
                             num_brokers=4,
                             log_config=log_config)

    def get_missing_node_idx(self, lhs: list[int], rhs: list[int]):
        missing_nodes = set(lhs).difference(set(rhs))
        assert len(missing_nodes) == 1, "Expected one missing node"
        return missing_nodes.pop()

    def generate_reassignments(self, all_node_idx, initial_assignments):
        reassignments_json = {"version": 1, "partitions": []}
        log_dirs = ["any" for _ in range(self.PARTITION_COUNT)]

        for assignment in initial_assignments:
            for partition in assignment.partitions:
                assert len(partition.replicas) == self.REPLICAS_COUNT
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, partition.replicas)
                # Replace one of the replicas with the missing one
                new_replica_assignment = partition.replicas
                new_replica_assignment[0] = missing_node_idx
                reassignments_json["partitions"].append({
                    "topic":
                    assignment.name,
                    "partition":
                    partition.id,
                    "replicas":
                    new_replica_assignment,
                    "log_dirs":
                    log_dirs
                })
                self.logger.debug(
                    f"{assignment.name} partition {partition.id}, new replica assignments: {new_replica_assignment}"
                )

        return reassignments_json

    @cluster(num_nodes=4)
    def test_reassignments_kafka_cli(self):
        initial_assignments = self.client().describe_topics()
        self.logger.debug(f"Initial assignments: {initial_assignments}")

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: {all_node_idx}")

        reassignments_json = self.generate_reassignments(
            all_node_idx, initial_assignments)

        kafka_tools = KafkaCliTools(self.redpanda)
        output = kafka_tools.execute_reassign_partitions(
            reassignments=reassignments_json)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        output = kafka_tools.verify_reassign_partitions(
            reassignments=reassignments_json)
        nodes_as_str = [str(n) for n in all_node_idx]
        check_verify_reassign_partitions(output, reassignments_json,
                                         nodes_as_str, self.logger)

    @cluster(num_nodes=4)
    def test_reassignments(self):
        # Initial replica assignments
        topic_names = [spec.name for spec in self.topics]
        topic_partitions = [p for p in range(self.PARTITION_COUNT)]
        kcl = KCL(self.redpanda)

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: {all_node_idx}")

        partitions_to_reassign = {
            topic_names[0]: topic_partitions,
            topic_names[1]: topic_partitions
        }

        def reassignments_done():
            responses = kcl.list_partition_reasssignments()
            self.logger.debug(responses)

            for res in responses:
                assert res.topic in partitions_to_reassign
                assert type(res.partition) == int
                assert res.partition in partitions_to_reassign[res.topic]
                assert set(res.replicas).issubset(set(all_node_idx))

                # Retry if any topic_partition has ongoing reassignments
                if len(res.adding_replicas) > 0 or len(
                        res.removing_replicas) > 0:
                    return False
            return True

        responses = kcl.list_partition_reasssignments(partitions_to_reassign)
        self.logger.debug(f"Initial assignments: {responses}")

        reassignments = {}
        for res in responses:
            assert res.topic in topic_names
            assert type(res.partition) == int
            assert res.partition in topic_partitions
            assert len(res.replicas) == self.REPLICAS_COUNT

            if res.topic not in reassignments:
                reassignments[res.topic] = {}

            assert res.partition not in reassignments[res.topic]
            # Add a node to the replica set
            missing_node_idx = self.get_missing_node_idx(
                all_node_idx, res.replicas)
            # Trigger replica add and removal by replacing one of the replicas
            res.replicas[0] = missing_node_idx
            reassignments[res.topic][res.partition] = res.replicas

        self.logger.debug(
            f"Replacing replicas. New assignments: {reassignments}")
        kcl.alter_partition_reasssignments(reassignments)
        wait_until(reassignments_done, timeout_sec=10, backoff_sec=1)

        def try_even_replication_factor(topics):
            try:
                kcl.alter_partition_reasssignments(topics)
                raise Exception(
                    "Even replica count accepted but it should be rejected")
            except RuntimeError as ex:
                if str(ex) == "Number of replicas != topic replication factor":
                    pass
                else:
                    raise

        # Test even replica count by adding a replica. Expect success because
        # the Redpanda partition allocator will replace one of the previous replicas
        # with the added replica.
        for topic in reassignments:
            for pid in reassignments[topic]:
                # Add a node to the replica set
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, reassignments[topic][pid])
                reassignments[topic][pid].append(missing_node_idx)

        self.logger.debug(
            f"Even replica count by adding. Expect fail. New assignments: {reassignments}"
        )

        try_even_replication_factor(reassignments)

        # Test even replica count by removing two replicas. Expect a failure
        # because even replication factor is not supported in Redpanda
        for topic in reassignments:
            for pid in reassignments[topic]:
                reassignments[topic][pid].pop()
                reassignments[topic][pid].pop()

        self.logger.debug(
            f"Even replica count by removal. Expect fail. New assignments: {reassignments}"
        )
        try_even_replication_factor(reassignments)

    def make_producer(self, topic_name, msg_size=512 * 1024, msg_count=1024):
        prod = RpkProducer(self.test_context,
                           self.redpanda,
                           topic_name,
                           msg_size=msg_size,
                           msg_count=msg_count)
        prod.start()
        return prod

    @cluster(num_nodes=6)
    def test_reassignments_cancel(self):
        initial_assignments = self.client().describe_topics()
        self.logger.debug(f"Initial assignments: {initial_assignments}")

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: {all_node_idx}")

        # Set a low throttle to slowdown partition move enough that there is
        # something to cancel
        self.redpanda.set_cluster_config({"raft_learner_recovery_rate": "10"})

        producers = [
            self.make_producer(self.topics[0].name),
            self.make_producer(self.topics[1].name)
        ]

        reassignments_json = self.generate_reassignments(
            all_node_idx, initial_assignments)

        kafka_tools = KafkaCliTools(self.redpanda)
        output = kafka_tools.execute_reassign_partitions(
            reassignments=reassignments_json)
        self.logger.debug(output)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        output = kafka_tools.cancel_reassign_partitions(
            reassignments=reassignments_json)
        check_cancel_reassign_partitions(output, reassignments_json,
                                         self.logger)

        output = kafka_tools.verify_reassign_partitions(
            reassignments=reassignments_json)
        nodes_as_str = [str(n) for n in all_node_idx]
        check_verify_reassign_partitions(output, reassignments_json,
                                         nodes_as_str, self.logger)

        for prod in producers:
            prod.wait()

    @cluster(num_nodes=5)
    def test_reassignments_on_stopped_nodes(self):
        initial_assignments = self.client().describe_topics()
        self.logger.debug(f"Initial assignments: {initial_assignments}")
        for assignment in initial_assignments:
            self.logger.debug(f'Initial {assignment}')

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: {all_node_idx}")

        reassignments_json = {"version": 1, "partitions": []}
        log_dirs = ["any" for _ in range(self.REPLICAS_COUNT)]

        assignment = initial_assignments[0]
        assert len(assignment.partitions) > 0
        partition = assignment.partitions[0]
        stop_node_id = self.get_missing_node_idx(all_node_idx,
                                                 partition.replicas)
        replaced_node_id = partition.replicas[0]
        partition.replicas[0] = stop_node_id
        reassignments_json["partitions"].append({
            "topic": assignment.name,
            "partition": partition.id,
            "replicas": partition.replicas,
            "log_dirs": log_dirs
        })
        self.logger.debug(f"Assignment with stop node {reassignments_json}")

        stopped_node = next(
            filter(lambda n: self.redpanda.node_id(n) == stop_node_id,
                   self.redpanda.nodes))
        self.logger.debug(f"Stopping node {stop_node_id}")
        self.redpanda.stop_node(stopped_node)

        self.logger.debug(
            "Execute reassignment with stopped node in the replica set")
        kafka_tools = KafkaCliTools(self.redpanda)

        # Either two of the following errors could surface first from the Kafka CLI.
        # The unknown broker error comes from Kafka's ReassignPartitionsCommand.scala
        no_broker_re = re.compile(r"^Error: Unknown broker id [0-9]+.*")
        # Failed connection comes from Kafka's NetworkClient.java
        no_connection_re = re.compile(
            r".*Connection to node [0-9]+ .* could not be established.*")

        try:
            kafka_tools.execute_reassign_partitions(
                reassignments=reassignments_json)
            raise Exception(
                f'AlterPartition w/ stopped node {stop_node_id} passed. Expected fail.'
            )
        except subprocess.CalledProcessError as e:
            no_broker_m = no_broker_re.match(e.output)
            no_conn_m = no_connection_re.match(e.output)
            assert no_broker_m is not None or no_conn_m is not None

        self.logger.debug(f"Restarting node {stop_node_id}")
        self.redpanda.restart_nodes([stopped_node])

        # Set a low throttle to slowdown partition move enough that there is
        # something to cancel
        self.redpanda.set_cluster_config({"raft_learner_recovery_rate": "10"})

        producer = self.make_producer(assignment.name)

        time.sleep(1)

        # Now test what happens when we cancel a request such that one of the nodes
        # in the restored replica set is down
        # So, let's stop a node while the partition move is running
        output = kafka_tools.execute_reassign_partitions(
            reassignments=reassignments_json)
        self.logger.debug(output)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        replaced_node = next(
            filter(lambda n: self.redpanda.node_id(n) == replaced_node_id,
                   self.redpanda.nodes))
        self.logger.debug(f"Stopping node {replaced_node_id}")
        self.redpanda.stop_node(replaced_node)

        self.logger.debug(
            "sleeping now -- look at redpanda logs for something")
        time.sleep(1)

        # try:
        # Now cancel the reassignment. The restored replica set will include
        # the replaced node that was stopped above
        output = kafka_tools.cancel_reassign_partitions(
            reassignments=reassignments_json)
        self.logger.debug(f'Nyalia TOPIC {assignment.name} -- {output}')
        check_cancel_reassign_partitions(output, reassignments_json,
                                         self.logger)
        #     raise Exception(
        #         f'AlterPartition cancel w/ stopped node {replaced_node_id} passed. Expected fail.'
        #     )
        # except subprocess.CalledProcessError as e:
        #     no_broker_m = no_broker_re.match(e.output)
        #     no_conn_m = no_connection_re.match(e.output)
        #     self.logger.debug(f'Nyalia {no_broker_m} - {no_conn_m} -- out {e.output}')
        #     if no_broker_m is not None or no_conn_m is not None:
        #         pass
        #     else:
        #         raise

        # Reset throttle rate so the move can complete
        # self.redpanda.set_cluster_config(
        #     {"raft_learner_recovery_rate": "100000000"})

        after_cancel_assignments = self.client().describe_topics()
        for assignment in after_cancel_assignments:
            self.logger.debug(f'After cancel {assignment}')

        output = kafka_tools.verify_reassign_partitions(
            reassignments=reassignments_json)
        nodes_as_str = [str(n) for n in all_node_idx]
        check_verify_reassign_partitions(output, reassignments_json,
                                         nodes_as_str, self.logger)

        after_verify_assignments = self.client().describe_topics()
        for assignment in after_verify_assignments:
            self.logger.debug(f'After Verify {assignment}')

        producer.wait()


class PartitionReassignmentsACLsTest(RedpandaTest):
    topics = [TopicSpec()]
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, test_context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = 'sasl'
        super(PartitionReassignmentsACLsTest,
              self).__init__(test_context=test_context,
                             num_brokers=4,
                             log_config=log_config,
                             security=security)

    @cluster(num_nodes=4)
    def test_reassignments_with_acls(self):
        admin = Admin(self.redpanda)
        username = "regular_user"
        admin.create_user(username, self.password, self.algorithm)

        # wait for user to propagate to nodes
        def user_exists():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
                if username not in users:
                    return False
            return True

        wait_until(user_exists, timeout_sec=10, backoff_sec=1)

        # Only one partition in this test and it doesn't matter if a
        # partition is moved so we hardcode in the replica set assignment
        partitions_to_reassign = {self.topic: [0]}
        reassignments = {self.topic: {0: [1, 2, 3]}}
        self.logger.debug(f"New replica assignments: {reassignments}")

        kcl = KCL(self.redpanda)
        user_cred = {
            "user": username,
            "passwd": self.password,
            "method": self.algorithm.lower().replace('-', '_')
        }
        try:
            kcl.alter_partition_reasssignments(reassignments,
                                               user_cred=user_cred)
            raise Exception(
                f'AlterPartition w/ user {user_cred} passed. Expected fail.')
        except subprocess.CalledProcessError as e:
            if e.output.startswith("CLUSTER_AUTHORIZATION_FAILED"):
                pass
            else:
                raise

        try:
            kcl.list_partition_reasssignments(partitions_to_reassign,
                                              user_cred=user_cred)
            raise Exception(
                f'ListPartition w/ user {user_cred} passed. Expected fail.')
        except subprocess.CalledProcessError as e:
            if e.output.startswith("CLUSTER_AUTHORIZATION_FAILED"):
                pass
            else:
                raise

        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        user_cred = {
            "user": super_username,
            "passwd": super_password,
            "method": super_algorithm.lower().replace('-', '_')
        }
        kcl.alter_partition_reasssignments(reassignments, user_cred=user_cred)

        def reassignments_done():
            responses = kcl.list_partition_reasssignments(
                partitions_to_reassign, user_cred=user_cred)
            self.logger.debug(responses)
            assert len(responses) == 1
            res = responses.pop()
            return res.replicas == reassignments[self.topic][0]

        wait_until(reassignments_done, timeout_sec=10, backoff_sec=1)
