#!/usr/bin/env python3

import worker_functions.constants as constants
import worker_functions.connection_aux_functions as cf
from worker_functions.zk_client import ZkClient

import threading
import logging
import copy


class ZkAdapterClient(ZkClient):
    def __init__(self, zookeeper_servers, username, password, ca_cert, logger):
        """
        Initializes zookeeper adapter client.
        :param zookeeper_servers: list of zookeeper servers to connect to
        :param username: username for zookeeper authentication
        :param password: password for zookeeper authentication
        :param ca_cert: CA certificate for zookeeper SSL verification and
            connection encrtyption
        :param logger: logger instance to use
        """
        super().__init__(
            zookeeper_servers=zookeeper_servers,
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )

        # mq servers
        self.mq_servers_lock = threading.Lock()
        self.mq_servers = []
    
    def __del__(self):
        super().__del__()
    
    def zk_callback_update_mq_servers(self, servers):
        """
        Zookeeper callback for updating MQ server list.
        :param servers: list of servers
        """
        self._set_mq_servers(cf.server_list(servers))
    
    def register_update_mq_server_callback(self):
        """
        Registers zk_callback_update_mq_servers callback in zookeeper.
        """
        self.zk.ChildrenWatch(
            path=constants.WORKER_CONFIG_MQ_SERVERS,
            func=self.zk_callback_update_mq_servers
        )
    
    def _set_mq_servers(self, servers):
        """
        Sets server list.
        :param servers: new list of MQ servers
        """
        self.mq_servers_lock.acquire()
        self.mq_servers = servers
        self.mq_servers_lock.release()

    def get_mq_servers(self):
        """
        Returns current copy of MQ server list
        :return: MQ server list
        """
        self.mq_servers_lock.acquire()
        mq_servers = copy.deepcopy(self.mq_servers)
        self.mq_servers_lock.release()
        return mq_servers
    
    def run(self):
        """
        Runs the worker adapter zookeeper client.
        """
        self.zk_connect()
        self.register_update_mq_server_callback()
