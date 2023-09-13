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


class WorkerAdapter(MQClient):
    """
    Worker adapter for connecting application to worker processing system.
    Adapter provides methods for sending data to and receiving data from the
    processing system.
    """
    def __init__(
        self,
        zk_client:           object,
        mail_client:         object,
        mail_subject_prefix: str,
        username:            str,
        password:            str,
        ca_cert:             str,
        recovery_timeout:     int = 60,
        max_error_count:     int = 10,
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
        :param mail_subject_prefix: prefix for mail subject field
        :param username: username for MQ authenticaiton
        :param password: password for MQ authentication
        :param ca_cert: CA certificate for MQ authentication and encryption
        :param recovery_timeout: timeout in seconds to wait before retrying
            operation that failed.
        :param max_error_count: maximum number of errors until email
            notification is sent.
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
        self.mail_subject_prefix = mail_subject_prefix
        self.recovery_timeout = recovery_timeout
        self.error_count = 0
        self.max_error_count = 10
    
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
        except Exception:
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
        while True:
            try:
                try:
                    self.mq_connect_retry(
                        confirm_delivery=True,
                        max_retry=self.max_error_count + 1,
                        retry_interval=self.recovery_timeout
                    )
                except ConnectionError:
                    self.logger.error(
                        'Failed to connect to MQ servers, trying again!'
                    )
                    self.mail_client.send_mail_notification(
                        subject=f'{self.mail_subject_prefix}'
                                ' - MQ connection error!',
                        body=f'{e}'
                    )
                    self.error_count += self.max_error_count + 1
                    continue

                self.mq_channel.basic_consume(
                    queue_name,
                    self._mq_receive_result_callback,
                    auto_ack = False
                )

                try:
                    self.mq_channel.start_consuming()
                except pika.exceptions.AMQPError as e:
                    # connection failed - continue / recover
                    self.logger.error(
                        'Result receiving failed due to MQ connection error!'
                    )
                    self.logger.debug(f'Received error: {e}')
                    if self.error_count > self.max_error_count:
                        self.mail_client.send_mail_notification(
                            subject=f'{self.mail_subject_prefix}'
                                    ' - MQ connection error!',
                            body=f'{e}'
                        )
                    self.error_count += 1
                except WorkerAdapterRecoverableError as e:
                    # result could not be processed by adapter logic - recoverable
                    error = traceback.format_exc()
                    self.logger.error('Failed to receive result!')
                    self.logger.debug(f'Received error:\n{error}')
                    if self.error_count > self.max_error_count:
                        self.mail_client.send_mail_notification(
                            subject=f'{self.mail_subject_prefix}'
                                    ' - Adapter error count exceeded!',
                            body=f'{error}'
                        )
                    self.error_count += 1
                    time.sleep(self.recovery_timeout)
            
            except KeyboardInterrupt:
                # prevent keyboard interrupt generating error messages
                self.logger.info('Result receiving stopped!')
                break
            except Exception:
                error = traceback.format_exc()
                self.logger.error(
                    'Unrecoverable error has occured during result receiving!'
                )
                self.logger.error(f'Received error:\n{error}')
                self.mail_client.send_mail_notification(
                    subject=f'{self.mail_subject_prefix}'
                            ' - Unrecoverable error has occured during'
                            ' result receiving!',
                    body=f'{error}'
                )
                self.error_count += 1
                return_code = 1
                break
        
        return return_code
    
    def mq_send_request(self,
        processing_request: ProcessingRequest,
        output_queue_name: str
    ):
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

    def start_uploading_requests(self,
            output_queue_name: str,
            get_request:       callable,
            confirm_send:      callable,
            report_error:      callable,
        ):
        """
        Periodically uploads processing requests to MQ.
        Processing reuqests are taken from method get_request passed
        to the constructor. After request is successfully sent to MQ, method
        confirm_send is called with ProcessingRequest instance as argument.
        :param output_queue_name: name of the output queue to which processed
            results should be uploaded to.
        :param get_request: method that returns ProcessingRequest
            instance to send to MQ.
        :param confirm_send: method called after ProcessingRequest is
            successfully send to MQ. Method must take ProcessingRequest
            instance as its argument.
        :param report_error: method called when sending of ProcessingRequest
            fails due to unrecoverable error. Method must take ProcessingRequest
            instance and string error message as arguments.
        :returns: execution status (0 == ok, else failed)
        """
        return_code = 0
        self.logger.info('Request uploading started!')
        while True:
            # watch for keyboard interrupt
            try:
                # get processing request
                try:
                    processing_request = get_request()
                except WorkerAdapterRecoverableError:
                    error = traceback.format_exc()
                    self.logger.error(
                        'Failed to get processing request, recovering!'
                    )
                    self.logger.error(f'Received error:\n{error}')
                    if self.error_count > self.max_error_count:
                        self.mail_client.send_mail_notification(
                            subject=f'{self.mail_subject_prefix}'
                                    ' - Failed to get processing request!',
                            body=f'{error}'
                        )
                    self.error_count += 1
                    time.sleep(self.recovery_timeout)
                    continue

                # no request is ready for processing
                if not processing_request:
                    continue

                # connect to MQ
                try:
                    self.mq_connect_retry(
                        confirm_delivery=True,
                        max_retry=self.max_error_count + 1,
                        retry_interval=self.recovery_timeout
                    )
                except ConnectionError:
                    self.logger.error(
                        'Failed to connect to MQ servers, trying again!'
                    )
                    self.mail_client.send_mail_notification(
                        subject=f'{self.mail_subject_prefix}'
                                ' - MQ connection error!',
                        body=f'{e}'
                    )
                    self.error_count += self.max_error_count + 1
                    continue

                # upload processing request
                try:
                    timestamp = self.mq_send_request(
                        processing_request=processing_request,
                        output_queue_name=output_queue_name
                    )
                except pika.exceptions.UnroutableError as e:
                    self.logger.error(
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' due to wrong input queue in configuration!'
                    )
                    self.logger.debug(f'Received error:\n{e}')
                    self.mail_client.send_mail_notification(
                        subject=f'{self.mail_subject_prefix}'
                                ' - Failed to upload page'
                                f' {processing_request.page_uuid} due to wrong '
                                'input queue in configuration!',
                        body=f'{e}'
                    )
                    report_error(
                        processing_request,
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' due to wrong input queue in configuration!\n'
                        f'Received error: {str(e)}'
                    )
                except pika.exceptions.AMQPError as e:
                    self.logger.error(
                        'Failed to upload processing request due to'
                        ' MQ connection error!'
                    )
                    self.logger.debug(f'Received error: {e}')
                    if self.error_count > self.max_error_count:
                        self.mail_client.send_mail_notification(
                            subject=f'{self.mail_subject_prefix}'
                                    ' - MQ connection error!',
                            body=f'{e}'
                        )
                    self.error_count += 1
                    time.sleep(self.recovery_timeout)
                except KeyboardInterrupt:
                    # prevent keyboard interrupt generating error messages
                    raise
                except Exception:
                    error = traceback.format_exc()
                    self.logger.error(
                        f'Failed to upload page {processing_request.page_uuid}'
                        ' to MQ!'
                    )
                    self.logger.error(f'Received error:\n{error}')
                    self.mail_client.send_mail_notification(
                        subject=f'{self.mail_subject_prefix} - Failed to upload'
                                f' page {processing_request.page_uuid} to MQ '
                                'due to unrecoverable error!',
                        body=f'{error}'
                    )
                    self.error_count += 1
                    return_code = 1
                    break
                else:
                    # update page after successfull upload
                    confirm_send(processing_request, timestamp)
                    self.error_count = 0
                    self.logger.info(
                        f'Page {processing_request.page_uuid}'
                        ' uploaded successfully!'
                    )
            except KeyboardInterrupt:
                # prevent keyboard interrupt generating error messages
                self.logger.info('Request uploading stopped!')
                break
            except Exception:
                error = traceback.format_exc()
                self.logger.error(
                    'Unrecoverable error has occured during request uploading!'
                )
                self.logger.error(f'Received error:\n{error}')
                self.mail_client.send_mail_notification(
                    subject=f'{self.mail_subject_prefix}'
                            ' - Unrecoverable error has occured during'
                            ' request uploading!',
                    body=f'{error}'
                )
                self.error_count += 1
                return_code = 1
                break
        
        return return_code
