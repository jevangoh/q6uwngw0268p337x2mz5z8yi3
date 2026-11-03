from typing import TypeAlias
from uuid import UUID as Uuid
from json import loads

from redis import Redis
from redis.client import PubSub as Subscriber
from PySide6.QtCore import QThread, Signal, QObject

RequestId: TypeAlias = Uuid
ObjectId: TypeAlias = Uuid


class _UpdatedSignal(QObject):
    updated = Signal(RequestId)


class Thread(QThread):
    def __init__(self, host: str, port: int):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.redis: Redis = Redis(host=host, port=port, decode_responses=True)
        self.active_objects: dict[str, _UpdatedSignal] = {}

    def register(self, object_id: str) -> None:
        self.active_objects[object_id] = _UpdatedSignal()

    def unregister(self, object_id: str) -> None:
        self.active_objects.pop(object_id, None)

    def run(self) -> None:
        _subscriber_: Subscriber = self.redis.pubsub()  # type: ignore[no-untyped-call]
        _subscriber_.subscribe("updated")

        try:
            while True:
                _message_ = _subscriber_.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if _message_ is None:
                    continue
                if _message_["type"] != "message":
                    continue
                _channel_: str = _message_["channel"]
                if _channel_ == "updated":
                    _object_id_: ObjectId = ObjectId(loads(_message_["data"])["object_id"])
                    _request_id_: RequestId = RequestId(loads(_message_["data"])["request_id"])
                    if _object_id_ in self.active_objects:
                        self.active_objects[_object_id_].updated.emit(_request_id_)
        finally:
            _subscriber_.close()
            self.redis.close()


q6uwngw0268p337x2mz5z8yi3: Thread = Thread(host="localhost", port=6379)
q6uwngw0268p337x2mz5z8yi3.start()
