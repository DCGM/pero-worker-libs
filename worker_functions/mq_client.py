#!/usr/bin/env python3

# MQ client

import pika
import logging
import ssl
import time
import traceback

import worker_functions.connection_aux_functions as cf
import worker_functions.constants as constants

class MQClient:
    """
    Blocking MQ client
    """
    def __init__(self, mq_servers = [], username = '', password = '', ca_cert = None, logger = logging.getLogger(__name__)):
        # list of mq servers
        self.mq_servers = mq_servers

        # ssl/tls
        if ca_cert:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(ca_cert)
            self.ssl_options = pika.SSLOptions(context)
        else:
            self.ssl_options = pika.connection.ConnectionParameters.DEFAULT_SSL_OPTIONS

        # authentication
        if username:
            self.mq_auth = pika.credentials.PlainCredentials(str(username), str(password))
        else:
            self.mq_auth = pika.connection.ConnectionParameters.DEFAULT_CREDENTIALS

        # logger
        self.logger = logger

        # connection properties
        self.mq_connection = None
        self.mq_channel = None
    
    def get_mq_servers(self):
        """
        MQ server list getter.
        :return: list of configured MQ servers
        """
        return self.mq_servers

    def mq_connect(self, heartbeat = 60):
        """
        Connect to message broker
        """
        for server in self.get_mq_servers():
            try:
                self.logger.info('Connecting to MQ server {}'.format(cf.host_port_to_string(server)))
                self.mq_connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=server['host'],
                        port=server['port'] if server['port'] else pika.connection.ConnectionParameters.DEFAULT_PORT,
                        ssl_options=self.ssl_options,
                        credentials=self.mq_auth,
                        heartbeat=heartbeat,
                        virtual_host=constants.MQ_VHOST
                    )
                )
                self.logger.info('Opening channel to MQ.')
                self.mq_channel = self.mq_connection.channel()
            except pika.exceptions.AMQPError as e:
                self.logger.error('MQ connection failed! Received error: {}'.format(e))
                continue
            else:
                self.logger.info('MQ connection and channel opened successfully!')
                break
    
    def mq_connect_retry(self, *, heartbeat = 60, max_retry = 0, retry_interval = 30):
        """
        Connect to message broker, retry connection until success or max_retry count is reached
        :param heartbeat: connection heartbeat interval (keepalive message to prevent disconnect)
        :param max_retry: maximum number of connection retries before giving up (0 = try forever)
        :param retry_interval: interval between connection retries
        :raise: ConnectionError if maximum number of retries is reached
        """
        retry_count = 0

        while not (self.mq_connection and self.mq_connection.is_open and self.mq_channel and self.mq_channel.is_open):
            # wait before retry
            if retry_count != 0:
                self.logger.warning(
                    'Failed to connect to MQ servers, waiting for {n} seconds to retry!'
                    .format(n=retry_interval)
                )
                time.sleep(retry_interval)
            
            # Connect to MQ servers
            self.mq_connect(heartbeat=heartbeat)
            retry_count += 1

            # maximum number of retries reached
            if max_retry and retry_count == max_retry:
                break
        
        if not (self.mq_connection and self.mq_connection.is_open and self.mq_channel and self.mq_channel.is_open):
            raise ConnectionError('Failed to connect to MQ servers, maximum number of retries reached!')
    
    def mq_disconnect(self):
        """
        Stops connection to message broker
        :raise: ValueError when connection or channel is broken due to timeout
        """
        if self.mq_channel and self.mq_channel.is_open:
            self.mq_channel.close()
        
        if self.mq_connection and self.mq_connection.is_open:
            self.mq_connection.close()
    
    def __del__(self):
        try:
            self.mq_disconnect()
        except (ValueError, pika.exceptions.AMQPError):
            self.logger.error('Failed to close connection to MQ, connection timed out!')
            self.logger.debug(f'Received error:\n{traceback.format_exc()}')
