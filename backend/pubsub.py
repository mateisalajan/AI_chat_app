import threading

class PubSub:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.subscriptions = {}
        self.lock = threading.Lock()

    def subscribe(self, channel, callback):
        with self.lock:
            self.subscriptions.setdefault(channel, []).append(callback)

    def publish(self, channel, message):
        # This just calls callbacks directly for now
        with self.lock:
            for cb in self.subscriptions.get(channel, []):
                cb(message)
