import ray


@ray.remote
class HaltSignalActor:
    def __init__(self):
        self.halt = False

    def send(self):
        self.halt = True

    def get(self):
        return self.halt

    def reset(self):
        self.halt = False
