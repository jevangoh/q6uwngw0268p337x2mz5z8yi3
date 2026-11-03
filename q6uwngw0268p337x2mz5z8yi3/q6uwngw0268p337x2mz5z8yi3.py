from typing import TypeAlias
from uuid import UUID as Uuid
from json import loads

from redis.client import PubSub as Subscriber
from PySide6.QtCore import QThread, Signal, QObject

import msjzm0ilrcvjjguhp29y62uzk as the_redis_client

RequestId: TypeAlias = Uuid
ObjectId: TypeAlias = Uuid


class _UpdatedSignal(QObject):
    updated = Signal(RequestId)


class Thread(QThread):
    def __init__(self):
        super().__init__()
        self.active_objects: dict[str, _UpdatedSignal] = {}

    def register(self, object_id: str) -> None:
        self.active_objects[object_id] = _UpdatedSignal()

    def unregister(self, object_id: str) -> None:
        self.active_objects.pop(object_id, None)

    def run(self) -> None:
        _subscriber_: Subscriber = the_redis_client._.pubsub()  # type: ignore[no-untyped-call]
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


q6uwngw0268p337x2mz5z8yi3: Thread = Thread()
q6uwngw0268p337x2mz5z8yi3.start()
