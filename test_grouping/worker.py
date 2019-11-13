class Worker:
    def __init__(self, id):
        self.ready = True
        self.dead = False
        self.busy = False
        self.id = id
