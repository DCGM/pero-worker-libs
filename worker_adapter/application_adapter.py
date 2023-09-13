#!/usr/bin/env python3

from abc import ABC, abstractmethod
from message_definitions.message_pb2 import ProcessingRequest

class ApplicationAdapter(ABC):
    """
    Application adapter - encapsulates application specific logic for worker
    adapter.
    """
    @abstractmethod
    def on_message_receive(self, processing_request: ProcessingRequest) -> None:
        """
        Method called by worker adapter when processed processing request
        is received.
        :param processing_request: processing request with results of processing
        :raise WorkerAdapterError when processing of the results fails
            unrecoverably
        :raise WorkerAdapterRecoverableError when processing of the results
            fails but should be tried again (the same request will be passed
            to the method again).
        """
        pass

    @abstractmethod
    def get_request(self) -> ProcessingRequest:
        """
        Method called by worker adapter when new processing request can be
        uploaded to MQ for processing.
        :return: processing request instance
        :raise WorkerAdapterError when unrecoverably fails
        :raise WorkerAdapterRecoverableError when fails but adapter should
            try to call the method later again
        """
        pass

    @abstractmethod
    def confirm_send(self,
        processing_request: ProcessingRequest,
        timestamp: datetime.datetime
    ) -> None:
        """
        Method called by worker adapter when processing request is succesfully
        send to MQ for processing. Processing request instance send is passed
        to this method as argument. Any acknowledgement or change of state of
        the request in database should happen in this method.
        :param processing_request: processing request that has been sent
        :param timestamp: timestamp when the request was sent
        :nothrow
        """
        pass

    @abstractmethod
    def report_error(self,
        processing_request: ProcessingRequest,
        traceback: str
    ) -> None:
        """
        Method called by worker adapter when processing request cannot be sent
        to processing and will not be tried to sent in the feature. This can
        happen when the request is malformed or processing stages does not
        exits. This method can be used to mark request as failed.
        :param processing_request: processing request that could not be send
        :param traceback: error traceback / error message
        :nothrow
        """
        pass
    
