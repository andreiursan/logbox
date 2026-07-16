from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class LogMessage(_message.Message):
    __slots__ = ("log_level", "logger", "mac", "message")
    LOG_LEVEL_FIELD_NUMBER: _ClassVar[int]
    LOGGER_FIELD_NUMBER: _ClassVar[int]
    MAC_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    log_level: str
    logger: str
    mac: bytes
    message: str
    def __init__(self, log_level: _Optional[str] = ..., logger: _Optional[str] = ..., mac: _Optional[bytes] = ..., message: _Optional[str] = ...) -> None: ...
