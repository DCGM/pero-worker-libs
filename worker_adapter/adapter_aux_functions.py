#!/usr/bin/env python3

# Contains auxiliary functions for worker adapter that are not directly part
# of the adapter

from message_definitions.message_pb2 import ProcessingRequest, Data

def create_processing_request(
    request_id: str,
    page_id: str,
    processing_stages: list[str],
    files: list[tuple[str, bytes]]
) -> ProcessingRequest:
    """
    Generates processing request from processing request data.
    :param request_id: UUID of request to process as string
    :param page_id: UUID of page to process as string
    :param processing_stages: list of processing stages request will go
        through
    :param files: list of tuples (file_name, file_content)
        file name must to be string
        file content must be bytes
    :return: ProcessingRequest instance
    """
    request = ProcessingRequest()
    request.uuid = request_id
    request.page_uuid = page_id
    request.priority = 0  # deprecated, always 0
    for stage in processing_stages:
        request.processing_stages.append(stage.strip())
    
    for file in files:
        f = request.results.add()
        f.name = file[0]
        f.content = file[1]
    
    return request
