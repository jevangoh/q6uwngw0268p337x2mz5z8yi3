from json import loads
from uuid import UUID as Uuid

import msjzm0ilrcvjjguhp29y62uzk as the_redis_client
from PySide6.QtCore import QObject, QThread, Signal
from redis.client import PubSub as Subscriber

type RequestId = Uuid
type ObjectId = Uuid


class _Signal(QObject):
    emitted = Signal(RequestId)


class Thread(QThread):
    def __init__(self) -> None:
        super().__init__()
        self.update_signals: dict[ObjectId, _Signal] = {}
        self.delete_signals: dict[ObjectId, _Signal] = {}

    def register(self, object_id: ObjectId) -> None:
        self.update_signals[object_id] = _Signal()
        self.delete_signals[object_id] = _Signal()

    def unregister(self, object_id: ObjectId) -> None:
        self.update_signals.pop(object_id, None)
        self.delete_signals.pop(object_id, None)

    def run(self) -> None:
        _subscriber_: Subscriber = the_redis_client._.pubsub()  # type: ignore[no-untyped-call]
        _subscriber_.subscribe("updated")
        _subscriber_.subscribe("deleted")

        try:
            while True:
                _message_ = _subscriber_.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if _message_ is None:
                    continue
                if _message_["type"] != "message":
                    continue
                _channel_: str = _message_["channel"]
                match _channel_:
                    case "updated":
                        _object_id_: ObjectId = ObjectId(loads(_message_["data"])["object_id"])
                        _request_id_: RequestId = RequestId(loads(_message_["data"])["request_id"])
                        if _object_id_ in self.update_signals:
                            self.update_signals[_object_id_].emitted.emit(_request_id_)
                    case "deleted":
                        _object_id_: ObjectId = ObjectId(loads(_message_["data"])["object_id"])
                        _request_id_: RequestId = RequestId(loads(_message_["data"])["request_id"])
                        if _object_id_ in self.delete_signals:
                            self.delete_signals[_object_id_].emitted.emit(_request_id_)

        finally:
            _subscriber_.close()


q6uwngw0268p337x2mz5z8yi3: Thread = Thread()
q6uwngw0268p337x2mz5z8yi3.start()
