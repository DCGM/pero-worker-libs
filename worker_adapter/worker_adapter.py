#!/usr/bin/env python3

from worker_functions.mq_client import MQClient
from message_definitions.message_pb2 import ProcessingRequest

from google.protobuf.timestamp_pb2 import Timestamp

import traceback
import datetime
import time
import pika
import logging


class WorkerAdapterError(Exception):
    """
    General exception type for the worker adapter.
    """


class WorkerAdapterRecoverableError(WorkerAdapterError):
    """
    Recoverable Error type for the worker adapter.
    This type of error should be thrown when error such as timeout, connection
    drop and other errors that can be recovered occurs.
    """


class BadRequestError(WorkerAdapterError):
    """
    Unrecoverable error occures when request does not have valid input queue,
    so it cannot be routed by message broker.
    """
    def __init__(self, message: str, request: ProcessingRequest = None):
        """
        :param message: error message to show.
        :param request: request that caused the exception
        """
        super().__init__(message)
        self.request = request


class WorkerAdapter(MQClient):
    """
    Worker adapter for connecting application to worker processing system.
    Adapter provides methods for sending data to and receiving data from the
    processing system.
    """
    def __init__(
        self,
        zk_client:           object,
        username:            str,
        password:            str,
        ca_cert:             str,
        connection_retry_interval:    int = 5,
        connection_max_retry_count:     int = 10,
        send_receive_retry_interval: int = 5,
        send_receive_max_retry_count: int = 5,
        mail_client: object = None,
        logger:              logging.Logger = logging.getLogger(__name__)
    ):
        """
        Init
        :param zk_client: zookeeper client object for synchronizing MQ server
            server list. Object must provide get_mq_servers() method that
            takes no arguments and returns list of MQ servers in 'output_record'
            format specified in 'worker_functions/connection_aux_functions.py'.
        :param mail_client: mail client providing
            send_mail_notification(subject, body) method to send email
            notification when max_error_count is exceeded.
        :param username: username for MQ authenticaiton
        :param password: password for MQ authentication
        :param ca_cert: CA certificate for MQ authentication and encryption
        :param logger: logger instance to use for log messages.
        """
        super().__init__(
            mq_servers=[],
            username=username,
            password=password,
            ca_cert=ca_cert,
            logger=logger
        )
        self.zk_client = zk_client
        self.mail_client = mail_client

        self.connection_retry_interval = connection_retry_interval
        self.connection_max_retry_count = connection_max_retry_count
        self.send_receive_retry_interval = send_receive_retry_interval
        self.send_receive_max_retry_count = send_receive_max_retry_count
    
    def __del__(self):
        super().__del__()
    
    def get_mq_servers(self):
        """
        Gets current list of MQ servers.
        :return: MQ server list
        """
        return self.zk_client.get_mq_servers()
    
    def _mq_receive_result_callback(self, channel, method, properties, body):
        """
        Callback function for receiving processed messages from message broker
        :param channel: channel from which the message is originated
        :param method: message delivery method
        :param properties: additional message properties
        :param body: message body (actual processing request/result)
        """
        try:
            processing_request = ProcessingRequest().FromString(body)
        except Exception as e:
            self.logger.error('Failed to parse received request!')
            self.logger.error(f'Received error:\n{traceback.format_exc()}')
            channel.basic_nack(
                delivery_tag = method.delivery_tag,
                requeue = True
            )
            raise

        try:
            self.on_message_receive(processing_request)
        except Exception as e:
            channel.basic_nack(
                delivery_tag = method.delivery_tag,
                requeue = True
            )
            raise
        else:
            channel.basic_ack(delivery_tag = method.delivery_tag)
    
    def start_receiving_results(self,
        queue_name: str,
        on_message_receive: callable
    ):
        """
        Receives results from MQ server from queue given by queue_name.
        :param queue_name: name of the queue to receive results from
        :param on_message_receive: method called when message is received.
            If message processing fails and should be tried again (for example
            due to connection error whith some external service like database),
            method can raise WorkerAdapterRecoverableError to make adapter wait
            for some time and then pull the same message from the MQ again.
        :return: execution status (0 == ok, else failed)
        """
        return_code = 0
        self.on_message_receive = on_message_receive
        self.logger.info('Result receiving started!')

        send_receive_retry_count = 0

        while True:
            try:
                self.mq_connect_retry(
                    confirm_delivery=True,
                    max_retry=self.connection_max_retry_count,
                    retry_interval=self.connection_retry_interval
                )

            except ConnectionError as e:
                self.logger.error(
                    'Failed to connect to MQ servers, trying again!'
                )
                if self.mail_client is not None:
                    self.mail_client.send_mail_notification(
                        subject=f'MQ connection error!',
                        body=f'{e}'
                    )
                raise WorkerAdapterError(
                    f'Failed to upload page {processing_request.page_uuid}'
                    ' to MQ!'
                ) from e

            try:
                self.mq_channel.basic_consume(
                    queue_name,
                    self._mq_receive_result_callback,
                    auto_ack = False
                )
                self.mq_channel.start_consuming()

            except pika.exceptions.AMQPError as e:
                # connection failed - continue / recover
                self.logger.error(
                    'Result receiving failed due to MQ connection error!'
                )
                self.logger.debug(f'{e}')

                if send_receive_retry_count >= self.send_receive_max_retry_count:
                    if self.mail_client is not None:
                        self.mail_client.send_mail_notification(
                            subject=f'MQ connection error!',
                            body=f'{e}'
                        )
                    raise WorkerAdapterError(
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' to MQ!'
                    ) from e
                send_receive_retry_count += 1
                time.sleep(self.send_receive_retry_interval)

            except WorkerAdapterRecoverableError as e:
                # result could not be processed by adapter logic - recoverable
                error = traceback.format_exc()
                self.logger.error('Failed to receive result!')
                self.logger.debug(f'Received error:\n{error}')
                if send_receive_retry_count > self.send_receive_max_retry_count:
                    if self.mail_client is not None:
                        self.mail_client.send_mail_notification(
                            subject=f'Adapter error count exceeded!',
                            body=f'{error}'
                        )
                    raise WorkerAdapterError(
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' to MQ!'
                    ) from e
                send_receive_retry_count += 1
                time.sleep(self.send_receive_retry_interval)
        
        return return_code
    
    def mq_send_request(self,
        processing_request: ProcessingRequest,
        output_queue_name: str
    ) -> datetime.datetime:
        """
        Sends processing request to the MQ server.
        :param processing_request: processing request instance to send
        :param output_queue_name: name of the output queue where request
            should be send after processing to be picked up
        :return: timestamp when request was send
        """
        # get input queue name (name of the first stage)
        input_queue_name = processing_request.processing_stages[0]

        # add output queue / stage to processing stages
        processing_request.processing_stages.append(output_queue_name)
        
        # generate input timestamp
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        Timestamp.FromDatetime(processing_request.start_time, timestamp)

        # send request
        self.mq_channel.basic_publish(
            exchange='',
            routing_key=input_queue_name,
            body=processing_request.SerializeToString(),
            properties=pika.BasicProperties(
                delivery_mode=2,
                priority=processing_request.priority
            ),
            mandatory=True
        )

        return timestamp

    def send_request(self,
            processing_request: ProcessingRequest,
            output_queue_name: str
        ) -> datetime.datetime:
        """
        Uploads processing request to MQ.
        :param processing_request: processing request to send.
        :param output_queue_name: name of the output queue to which processed
            results should be uploaded to.
        :returns: timestamp when request was send
        :raise BadRequestError when fails to upload processing_request to MQ
            due to wrong input queue in configuration.
        :raise WorkerAdapterError when unrecoverable error occurs during request
            uploading.
        """
        send_retry_count = 0

        while True:
            # connect to MQ
            try:
                self.mq_connect_retry(
                    confirm_delivery=True,
                    max_retry=self.connection_max_retry_count,
                    retry_interval=self.connection_retry_interval
                )

            except ConnectionError as e:
                self.logger.error(
                    'Failed to connect to MQ servers, trying again!'
                )
                if self.mail_client is not None:
                    self.mail_client.send_mail_notification(
                        subject=f'MQ connection error!',
                        body=f'{e}'
                    )
                raise WorkerAdapterError(
                    f'Failed to upload page {processing_request.page_uuid}'
                    ' to MQ!'
                ) from e

            # upload processing request
            try:
                timestamp = self.mq_send_request(
                    processing_request=processing_request,
                    output_queue_name=output_queue_name
                )

            except pika.exceptions.AMQPError as e:
                self.logger.error(
                    'Failed to upload processing request due to'
                    ' MQ connection error!'
                )
                self.logger.debug(f'{e}')
                if send_retry_count >= self.send_receive_max_retry_count:
                    if self.mail_client is not None:
                        self.mail_client.send_mail_notification(
                            subject=f'MQ connection error!',
                            body=f'{e}'
                        )
                    raise WorkerAdapterError(
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' to MQ!'
                    ) from e
                send_retry_count += 1
                time.sleep(self.send_receive_retry_interval)

            except pika.exceptions.UnroutableError as e:
                raise BadRequestError(
                    message=f'Failed to upload page {processing_request.page_uuid}'
                    ' due to wrong input queue in configuration!',
                    request=processing_request
                ) from e

            else:
                self.logger.info(
                    f'Page {processing_request.page_uuid}'
                    ' uploaded successfully!'
                )
                break
        
        return timestamp
