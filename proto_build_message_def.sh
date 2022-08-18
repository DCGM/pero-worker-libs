#!/bin/sh

# updates message definition using protoc compiler
# protoc v3.19.1 was used to build the message

protoc -I=$(pwd) -I=/usr/local/include/google/protobuf --python_out=$(pwd) message-definitions/message.proto
