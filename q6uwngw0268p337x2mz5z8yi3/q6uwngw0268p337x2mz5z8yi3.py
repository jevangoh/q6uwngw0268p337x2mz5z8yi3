from typing import TypeAlias

from uuid import UUID as RequestId
from json import loads
from queue import Queue, Empty
from threading import Event as AwaitEvent

from redis import Redis
from redis.client import PubSub as Subscriber
from PySide6.QtCore import QThread, Signal

Subscription: TypeAlias = tuple[AwaitEvent, str]


class Thread(QThread):
    object_updated = Signal(str, RequestId)  # object_id, request_id

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.redis: Redis = Redis(host=host, port=port, decode_responses=True)
        self.subscriptions_queue: Queue[Subscription] = Queue()
        self.unsubscriptions_queue: Queue[Subscription] = Queue()

    def subscribe(self, channel_name: str) -> None:
        _await_event_: AwaitEvent = AwaitEvent()
        self.subscriptions_queue.put((_await_event_, channel_name))
        _await_event_.wait()

    def unsubscribe(self, channel_name: str) -> None:
        _await_event_: AwaitEvent = AwaitEvent()
        self.unsubscriptions_queue.put((_await_event_, channel_name))
        _await_event_.wait()

    def run(self) -> None:
        _subscriber_: Subscriber = self.redis.pubsub()  # type: ignore[no-untyped-call]

        try:
            while True:
                while True:
                    try:
                        _await_event_, _channel_name_ = self.subscriptions_queue.get_nowait()
                        _subscriber_.subscribe(_channel_name_)
                        _await_event_.set()
                    except Empty:
                        break

                while True:
                    try:
                        _await_event_, _channel_name_ = self.unsubscriptions_queue.get_nowait()
                        _subscriber_.unsubscribe(_channel_name_)  # type: ignore[no-untyped-call]
                        _await_event_.set()
                    except Empty:
                        break

                _message_ = _subscriber_.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if _message_ is None:
                    continue
                if _message_["type"] != "message":
                    continue
                _channel_: str = _message_["channel"]
                if _channel_ == "request_completed":
                    # self.request_completed.emit(_message_["data"])
                    pass
                else:
                    _id_: str = _channel_.replace("_updated", "")
                    self.object_updated.emit(
                        _id_, RequestId(loads(_message_["data"])["request_id"])
                    )
        finally:
            _subscriber_.close()
            self.redis.close()


q6uwngw0268p337x2mz5z8yi3: Thread = Thread(host="localhost", port=6379)
q6uwngw0268p337x2mz5z8yi3.start()
