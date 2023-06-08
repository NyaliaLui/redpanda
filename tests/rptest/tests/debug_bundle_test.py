# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import hashlib
import os
import os.path
import random
import requests
import string
import subprocess
import time

from datetime import datetime
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, ResourceSettings, SecurityConfig
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest

DEBUG_BUNDLE_ALLOW_LIST = [
    r"ERROR.*debug_bundle.*Bundle creation terminated with signal 15"
]


class AdminAPIDebugBundleTest(RedpandaTest):
    topics = [TopicSpec()]
    """
    Test capturing a debug bundle with the Admin API
    """
    def __init__(self, *args, **kwargs):
        super(AdminAPIDebugBundleTest, self).__init__(
            *args,
            resource_settings=ResourceSettings(num_cpus=1),
            log_config=LoggingConfig('info',
                                     logger_levels={'debug_bundle': 'trace'}),
            **kwargs)

    def setUp(self):
        # Do startup in test method so security settings are set at runtime
        pass

    @cluster(num_nodes=5)
    @matrix(use_sasl=[False, True])
    def test_debug_bundle(self, use_sasl: bool):
        security = SecurityConfig()
        auth_tuple = None
        if use_sasl:
            security.enable_sasl = True
            security.endpoint_authn_method = "sasl"
            super_username, super_password, _ = self.redpanda.SUPERUSER_CREDENTIALS
            auth_tuple = (super_username, super_password)

        self.redpanda.set_security_settings(security)
        self.redpanda.start()
        self._create_initial_topics()

        msg_count = 1000
        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=12800,
                               msg_count=msg_count,
                               acks=-1,
                               auth_tuple=self.redpanda.SUPERUSER_CREDENTIALS
                               if use_sasl else None)
        producer.start()
        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topic,
                               group="debug-bundle-group",
                               num_msgs=msg_count,
                               auth_tuple=self.redpanda.SUPERUSER_CREDENTIALS
                               if use_sasl else None)
        consumer.start()

        wait_until(lambda: consumer.message_count > msg_count / 2,
                   timeout_sec=30,
                   backoff_sec=1)

        node = self.redpanda.started_nodes()[0]
        default_bundle_name = "debug-bundle.zip"

        admin = Admin(self.redpanda)
        journalctl_date = datetime.now().strftime("%Y-%m-%d")
        res = admin.start_debug_bundle(node=node,
                                       logs_since=journalctl_date,
                                       logs_size_limit="100MiB",
                                       metrics_interval="10s",
                                       auth=auth_tuple)
        assert res.status_code == requests.codes.accepted

        try:
            res = admin.start_debug_bundle(node=node, auth=auth_tuple)
            raise RuntimeError(
                f"Second bundle creation succeded, expected 429: HTTP Status {res.status_code}"
            )
        except HTTPError as ex:
            # Expect HTTP Error 429 since a bundle is already running
            if ex.response.status_code == requests.codes.too_many_requests:
                pass
            else:
                raise

        def bundle_status():
            try:
                res = admin.debug_bundle_status(filename=default_bundle_name,
                                                node=node,
                                                auth=auth_tuple)
                # OK status means the bundle is available for download
                return res.status_code == requests.codes.ok
            except HTTPError as ex:
                # Not found status means the bundle is in progress but not yet ready
                if ex.response.status_code == requests.codes.not_found:
                    return False
                else:
                    raise

        wait_until(bundle_status, timeout_sec=120, backoff_sec=10)

        def download_bundle(local_bundle_filename: str, n: ClusterNode,
                            auth_tuple: tuple):
            res = admin.download_debug_bundle(filename=default_bundle_name,
                                              node=n,
                                              auth=auth_tuple)
            assert res.status_code == requests.codes.ok
            with open(local_bundle_filename, "wb") as local_bundle:
                local_bundle.write(res.content)

            assert os.path.isfile(local_bundle_filename)

        # Download the bundle twice so we can compare the contents.
        # The file will be on the orchestrator node
        str_gen = lambda size: "".join(
            random.choice(string.ascii_lowercase) for _ in range(size))
        bundle1 = f"/tmp/bundle-{str_gen(8)}.zip"
        bundle2 = f'/tmp/{default_bundle_name}'
        download_bundle(bundle1, node, auth_tuple)
        node.account.sftp_client.get(bundle2, bundle2)
        result = subprocess.run(["diff", bundle1, bundle2],
                                capture_output=True,
                                text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"diff command failed: exit status {result.returncode}, stdout {result.stdout}, stderr {result.stderr}"
            )

        # Remove the remote and local copies of the bundle
        res = admin.remove_debug_bundle(filename=default_bundle_name,
                                        node=node,
                                        auth=auth_tuple)
        assert res.status_code == requests.codes.ok
        os.remove(bundle1)
        assert not os.path.exists(bundle1)
        os.remove(bundle2)
        assert not os.path.exists(bundle2)

        # Another status check should return 410 (Gone) status because the bundle
        # was removed from disk
        try:
            res = admin.debug_bundle_status(filename=default_bundle_name,
                                            node=node,
                                            auth=auth_tuple)
            raise RuntimeError(
                f"Bundle status succeded, expected 410: HTTP Status {res.status_code}"
            )
        except HTTPError as ex:
            if ex.response.status_code != requests.codes.gone:
                raise

        producer.wait()
        consumer.wait()
        producer.stop()
        consumer.stop()

    @cluster(num_nodes=5, allow_list=DEBUG_BUNDLE_ALLOW_LIST)
    def test_debug_bundle_abort(self):
        self.redpanda.start()
        self._create_initial_topics()

        msg_count = 1000
        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=12800,
                               msg_count=msg_count,
                               acks=-1)
        producer.start()
        consumer = RpkConsumer(self.test_context,
                               self.redpanda,
                               self.topic,
                               group="debug-bundle-group",
                               num_msgs=msg_count)
        consumer.start()

        wait_until(lambda: consumer.message_count > msg_count / 2,
                   timeout_sec=30,
                   backoff_sec=1)

        node = self.redpanda.started_nodes()[0]
        default_bundle_name = "debug-bundle.zip"

        admin = Admin(self.redpanda)
        res = admin.start_debug_bundle(node=node)
        assert res.status_code == requests.codes.accepted

        try:
            # Redpanda should report that bundle is not ready with 404 status
            res = admin.debug_bundle_status(filename=default_bundle_name,
                                            node=node)
            raise RuntimeError(
                f"Bundle status succeded, expected 404: HTTP Status {res.status_code}"
            )
        except HTTPError as ex:
            if ex.response.status_code != requests.codes.not_found:
                raise

        # Do abort
        res = admin.abort_debug_bundle(node=node)
        assert res.status_code == requests.codes.ok

        try:
            # Redpanda should report that bundle is not on disk with 410 status
            admin.debug_bundle_status(filename=default_bundle_name, node=node)
            raise RuntimeError(
                f"Bundle status succeded, expected 410: HTTP Status {res.status_code}"
            )
        except HTTPError as ex:
            if ex.response.status_code != requests.codes.gone:
                raise

        producer.wait()
        consumer.wait()
        producer.stop()
        consumer.stop()
