#!/usr/bin/env python3

from abc import ABC, abstractmethod
from message_definitions.message_pb2 import ProcessingRequest

import datetime

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
    
