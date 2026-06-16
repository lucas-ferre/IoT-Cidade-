from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeviceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DEVICE_TYPE_UNKNOWN: _ClassVar[DeviceType]
    DEVICE_TYPE_TRAFFIC_LIGHT: _ClassVar[DeviceType]
    DEVICE_TYPE_LAMP_POST: _ClassVar[DeviceType]
    DEVICE_TYPE_WEATHER_STATION: _ClassVar[DeviceType]
    DEVICE_TYPE_CAMERA: _ClassVar[DeviceType]
    DEVICE_TYPE_AIR_QUALITY: _ClassVar[DeviceType]

class DeviceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_UNKNOWN: _ClassVar[DeviceStatus]
    STATUS_ON: _ClassVar[DeviceStatus]
    STATUS_OFF: _ClassVar[DeviceStatus]
    STATUS_ERROR: _ClassVar[DeviceStatus]

class RequestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REQUEST_TYPE_UNKNOWN: _ClassVar[RequestType]
    REQUEST_TYPE_LIST_DEVICES: _ClassVar[RequestType]
    REQUEST_TYPE_SEND_COMMAND: _ClassVar[RequestType]
    REQUEST_TYPE_ANALYTICS_QUERY: _ClassVar[RequestType]

class QueryOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OP_UNKNOWN: _ClassVar[QueryOp]
    OP_AVERAGE: _ClassVar[QueryOp]
    OP_STD_DEV: _ClassVar[QueryOp]
    OP_MAX_VARIATION: _ClassVar[QueryOp]
    OP_ANOMALY_DETECTION: _ClassVar[QueryOp]
    OP_PERCENTILE_95: _ClassVar[QueryOp]
    OP_LINEAR_TREND: _ClassVar[QueryOp]
DEVICE_TYPE_UNKNOWN: DeviceType
DEVICE_TYPE_TRAFFIC_LIGHT: DeviceType
DEVICE_TYPE_LAMP_POST: DeviceType
DEVICE_TYPE_WEATHER_STATION: DeviceType
DEVICE_TYPE_CAMERA: DeviceType
DEVICE_TYPE_AIR_QUALITY: DeviceType
STATUS_UNKNOWN: DeviceStatus
STATUS_ON: DeviceStatus
STATUS_OFF: DeviceStatus
STATUS_ERROR: DeviceStatus
REQUEST_TYPE_UNKNOWN: RequestType
REQUEST_TYPE_LIST_DEVICES: RequestType
REQUEST_TYPE_SEND_COMMAND: RequestType
REQUEST_TYPE_ANALYTICS_QUERY: RequestType
OP_UNKNOWN: QueryOp
OP_AVERAGE: QueryOp
OP_STD_DEV: QueryOp
OP_MAX_VARIATION: QueryOp
OP_ANOMALY_DETECTION: QueryOp
OP_PERCENTILE_95: QueryOp
OP_LINEAR_TREND: QueryOp

class Metric(_message.Message):
    __slots__ = ("name", "value", "unit")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: float
    unit: str
    def __init__(self, name: _Optional[str] = ..., value: _Optional[float] = ..., unit: _Optional[str] = ...) -> None: ...

class AggregatorLoad(_message.Message):
    __slots__ = ("aggregator_id", "ip_address", "telemetry_port", "discovery_port", "cpu_load", "queue_size", "timestamp")
    AGGREGATOR_ID_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    TELEMETRY_PORT_FIELD_NUMBER: _ClassVar[int]
    DISCOVERY_PORT_FIELD_NUMBER: _ClassVar[int]
    CPU_LOAD_FIELD_NUMBER: _ClassVar[int]
    QUEUE_SIZE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    aggregator_id: str
    ip_address: str
    telemetry_port: int
    discovery_port: int
    cpu_load: float
    queue_size: int
    timestamp: int
    def __init__(self, aggregator_id: _Optional[str] = ..., ip_address: _Optional[str] = ..., telemetry_port: _Optional[int] = ..., discovery_port: _Optional[int] = ..., cpu_load: _Optional[float] = ..., queue_size: _Optional[int] = ..., timestamp: _Optional[int] = ...) -> None: ...

class DiscoveryResponse(_message.Message):
    __slots__ = ("message_id", "timestamp", "device_id", "type", "ip_address", "control_port", "initial_status", "is_controllable", "aggregator_id", "coord_x", "coord_y")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    INITIAL_STATUS_FIELD_NUMBER: _ClassVar[int]
    IS_CONTROLLABLE_FIELD_NUMBER: _ClassVar[int]
    AGGREGATOR_ID_FIELD_NUMBER: _ClassVar[int]
    COORD_X_FIELD_NUMBER: _ClassVar[int]
    COORD_Y_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    timestamp: int
    device_id: str
    type: DeviceType
    ip_address: str
    control_port: int
    initial_status: DeviceStatus
    is_controllable: bool
    aggregator_id: str
    coord_x: int
    coord_y: int
    def __init__(self, message_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., device_id: _Optional[str] = ..., type: _Optional[_Union[DeviceType, str]] = ..., ip_address: _Optional[str] = ..., control_port: _Optional[int] = ..., initial_status: _Optional[_Union[DeviceStatus, str]] = ..., is_controllable: bool = ..., aggregator_id: _Optional[str] = ..., coord_x: _Optional[int] = ..., coord_y: _Optional[int] = ...) -> None: ...

class DataPayload(_message.Message):
    __slots__ = ("message_id", "timestamp", "device_id", "current_status", "metrics", "coord_x", "coord_y")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STATUS_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    COORD_X_FIELD_NUMBER: _ClassVar[int]
    COORD_Y_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    timestamp: int
    device_id: str
    current_status: DeviceStatus
    metrics: _containers.RepeatedCompositeFieldContainer[Metric]
    coord_x: int
    coord_y: int
    def __init__(self, message_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., device_id: _Optional[str] = ..., current_status: _Optional[_Union[DeviceStatus, str]] = ..., metrics: _Optional[_Iterable[_Union[Metric, _Mapping]]] = ..., coord_x: _Optional[int] = ..., coord_y: _Optional[int] = ...) -> None: ...

class ConfigCommand(_message.Message):
    __slots__ = ("command_id", "timestamp", "update_status", "target_status", "update_frequency", "new_frequency_secs", "target_device_id")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    UPDATE_STATUS_FIELD_NUMBER: _ClassVar[int]
    TARGET_STATUS_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FREQUENCY_FIELD_NUMBER: _ClassVar[int]
    NEW_FREQUENCY_SECS_FIELD_NUMBER: _ClassVar[int]
    TARGET_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    timestamp: int
    update_status: bool
    target_status: DeviceStatus
    update_frequency: bool
    new_frequency_secs: int
    target_device_id: str
    def __init__(self, command_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., update_status: bool = ..., target_status: _Optional[_Union[DeviceStatus, str]] = ..., update_frequency: bool = ..., new_frequency_secs: _Optional[int] = ..., target_device_id: _Optional[str] = ...) -> None: ...

class ConfigResponse(_message.Message):
    __slots__ = ("message_id", "command_id", "timestamp", "success", "message", "updated_status", "updated_frequency_secs")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    UPDATED_STATUS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_FREQUENCY_SECS_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    command_id: str
    timestamp: int
    success: bool
    message: str
    updated_status: DeviceStatus
    updated_frequency_secs: int
    def __init__(self, message_id: _Optional[str] = ..., command_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., success: bool = ..., message: _Optional[str] = ..., updated_status: _Optional[_Union[DeviceStatus, str]] = ..., updated_frequency_secs: _Optional[int] = ...) -> None: ...

class DeviceInfo(_message.Message):
    __slots__ = ("device_id", "type", "status", "ip_address", "control_port", "is_controllable", "last_seen_timestamp", "coord_x", "coord_y")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    IP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CONTROL_PORT_FIELD_NUMBER: _ClassVar[int]
    IS_CONTROLLABLE_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    COORD_X_FIELD_NUMBER: _ClassVar[int]
    COORD_Y_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    type: DeviceType
    status: DeviceStatus
    ip_address: str
    control_port: int
    is_controllable: bool
    last_seen_timestamp: int
    coord_x: int
    coord_y: int
    def __init__(self, device_id: _Optional[str] = ..., type: _Optional[_Union[DeviceType, str]] = ..., status: _Optional[_Union[DeviceStatus, str]] = ..., ip_address: _Optional[str] = ..., control_port: _Optional[int] = ..., is_controllable: bool = ..., last_seen_timestamp: _Optional[int] = ..., coord_x: _Optional[int] = ..., coord_y: _Optional[int] = ...) -> None: ...

class ClientRequest(_message.Message):
    __slots__ = ("message_id", "timestamp", "type", "target_device_id", "command_payload", "query_metric", "query_op", "start_timestamp", "end_timestamp")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    QUERY_METRIC_FIELD_NUMBER: _ClassVar[int]
    QUERY_OP_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    timestamp: int
    type: RequestType
    target_device_id: str
    command_payload: ConfigCommand
    query_metric: str
    query_op: QueryOp
    start_timestamp: int
    end_timestamp: int
    def __init__(self, message_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., type: _Optional[_Union[RequestType, str]] = ..., target_device_id: _Optional[str] = ..., command_payload: _Optional[_Union[ConfigCommand, _Mapping]] = ..., query_metric: _Optional[str] = ..., query_op: _Optional[_Union[QueryOp, str]] = ..., start_timestamp: _Optional[int] = ..., end_timestamp: _Optional[int] = ...) -> None: ...

class DataPoint(_message.Message):
    __slots__ = ("timestamp", "value", "device_id")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: float
    device_id: str
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[float] = ..., device_id: _Optional[str] = ...) -> None: ...

class ClientResponse(_message.Message):
    __slots__ = ("message_id", "timestamp", "success", "message", "devices", "analytics_result", "result_metadata", "graph_points")
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DEVICES_FIELD_NUMBER: _ClassVar[int]
    ANALYTICS_RESULT_FIELD_NUMBER: _ClassVar[int]
    RESULT_METADATA_FIELD_NUMBER: _ClassVar[int]
    GRAPH_POINTS_FIELD_NUMBER: _ClassVar[int]
    message_id: str
    timestamp: int
    success: bool
    message: str
    devices: _containers.RepeatedCompositeFieldContainer[DeviceInfo]
    analytics_result: float
    result_metadata: str
    graph_points: _containers.RepeatedCompositeFieldContainer[DataPoint]
    def __init__(self, message_id: _Optional[str] = ..., timestamp: _Optional[int] = ..., success: bool = ..., message: _Optional[str] = ..., devices: _Optional[_Iterable[_Union[DeviceInfo, _Mapping]]] = ..., analytics_result: _Optional[float] = ..., result_metadata: _Optional[str] = ..., graph_points: _Optional[_Iterable[_Union[DataPoint, _Mapping]]] = ...) -> None: ...
