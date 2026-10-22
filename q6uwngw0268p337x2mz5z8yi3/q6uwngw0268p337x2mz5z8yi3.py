from queue import Queue, Empty
from redis import Redis
from redis.client import PubSub as Subscriber
from PySide6.QtCore import QThread, Signal


class Thread(QThread):
    object_updated = Signal(str)  # id

    def __init__(self, host: str, port: int):
        super().__init__()
        self.host: str = host
        self.port: int = port
        self.redis: Redis = Redis(host=host, port=port, decode_responses=True)
        self.subscriptions_queue: Queue = Queue()
        self.unsubscriptions_queue: Queue = Queue()

    def subscribe(self, channel_name: str):
        self.subscriptions_queue.put(channel_name)

    def unsubscribe(self, channel_name: str):
        self.unsubscriptions_queue.put(channel_name)

    def run(self):
        _subscriber_: Subscriber = self.redis.pubsub()

        try:
            while True:
                while True:
                    try:
                        _channel_name_ = self.subscriptions_queue.get_nowait()
                        _subscriber_.subscribe(_channel_name_)
                    except Empty:
                        break

                while True:
                    try:
                        _channel_name_ = self.unsubscriptions_queue.get_nowait()
                        _subscriber_.unsubscribe(_channel_name_)
                    except Empty:
                        break

                _message_ = _subscriber_.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if _message_ is None:
                    continue
                if _message_["type"] != "message":
                    continue
                _channel_: str = _message_["channel"]
                _id_: str = _channel_.replace("_updated", "")
                self.object_updated.emit(_id_)
        finally:
            _subscriber_.close()
            self.redis.close()


q6uwngw0268p337x2mz5z8yi3: Thread = Thread()
q6uwngw0268p337x2mz5z8yi3.start()
