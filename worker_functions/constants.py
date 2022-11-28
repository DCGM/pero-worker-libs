#!/usr/bin/env python3

# constants for pero worker

# zookeeper paths

PERO_PATH = '/pero'

WORKER_STATUS = PERO_PATH + '/worker/status'
WORKER_STATUS_ID_TEMPLATE = WORKER_STATUS + '/{worker_id}'
WORKER_STATUS_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/status'  # current worker status
WORKER_STATUS_CONNECTED_TEMPLATE = WORKER_STATUS_TEMPLATE + '/connected'  # indicates if worker is connected to zookeeper (should be always ephemeral node!)
WORKER_QUEUE_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/queue'  # input queue
WORKER_ENABLED_TEMPLATE = WORKER_STATUS_ID_TEMPLATE + '/enabled'  # worker enabled
WORKER_UNLOCK_TIME = WORKER_STATUS_ID_TEMPLATE + '/unlock_time'  # time after which it is possible to switch worker to another queue

WORKER_CONFIG = PERO_PATH + '/worker/config'  # configs for all workers
WORKER_CONFIG_MQ_SERVERS = WORKER_CONFIG + '/mq_servers'
WORKER_CONFIG_MQ_MONITORING_SERVERS = WORKER_CONFIG + '/mq_monitoring_servers'
WORKER_CONFIG_FTP_SERVERS = WORKER_CONFIG + '/ftp_servers'

QUEUE = PERO_PATH + '/queue'
QUEUE_TEMPLATE = QUEUE + '/{queue_name}'
QUEUE_CONFIG_TEMPLATE = QUEUE_TEMPLATE + '/config'  # configs for processing (config.ini)
QUEUE_CONFIG_PATH_TEMPLATE = QUEUE_TEMPLATE + '/config_path'  # path to config on FTP server
QUEUE_CONFIG_VERSION_TEMPLATE = QUEUE_TEMPLATE + '/config_version'  # version of the configuration
QUEUE_CONFIG_ADMINISTRATIVE_PRIORITY_TEMPLATE = QUEUE_TEMPLATE + '/administrative_priority'  # priority set by administrator to prefer queue
QUEUE_STATS_AVG_MSG_TIME_TEMPLATE = QUEUE_TEMPLATE + '/avg_msg_time'  # average time needed for processing message from this queue
QUEUE_STATS_AVG_MSG_TIME_LOCK_TEMPLATE = QUEUE_TEMPLATE + '/avg_msg_time_lock'  # lock for avg_msg_time access
QUEUE_STATS_WAITING_SINCE_TEMPLATE = QUEUE_TEMPLATE + '/waiting_since'  # time when oldest message was written to the queue

# zookeeper int data settings
ZK_INT_BYTEORDER = 'big'  # Big endian - MSB is at the beginning of the byte object

# rabbitmq vhost
MQ_VHOST = '/pero'

# worker status
STATUS_STARTING = 'STARTING'
STATUS_PROCESSING = 'PROCESSING'
STATUS_CONFIGURING = 'CONFIGURING'                    # changing configuration of stage / swithing stage
STATUS_CONNECTING = 'CONNECTING'                      # includes reconection attempts
STATUS_IDLE = 'IDLE'
STATUS_PROCESSING_FAILED = 'PROCESSING_FAILED'        # unknown processing failure
STATUS_CONNECTION_FAILED = 'CONNECTION_FAILED'        # connection failed and recconection attepts too
STATUS_CONFIGURATION_FAILED = 'CONFIGURATION_FAILED'  # failed to reconfigure worker / switch stage
STATUS_DEAD = 'DEAD'                                  # worker is not running
# backward compatibility
STATUS_RECONFIGURING = STATUS_CONFIGURING
STATUS_FAILED = STATUS_PROCESSING_FAILED
