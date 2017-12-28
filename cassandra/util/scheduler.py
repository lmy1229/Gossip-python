import random

import time
from apscheduler.schedulers.background import BackgroundScheduler
from multiprocessing import Process


class Scheduler(Process):
    def __init__(self, interval):
        Process.__init__(self)
        self.interval = interval  # in seconds

    def interval_task(self):
        raise NotImplementedError

    def main_task(self):
        raise NotImplementedError

    def run(self):
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.interval_task, 'interval', seconds=self.interval)
        scheduler.start()

        self.main_task()


if __name__ == '__main__':
    # test
    class Gossiper(Scheduler):

        def __init__(self, interval):
            super(Gossiper, self).__init__(interval)
            self.var = 0
            self.ver = 0

        def get(self):
            return self.var, self.ver

        def interval_task(self):
            self.ver += 1
            print("in runner: {}".format(self.get()))

        def main_task(self):
            while True:
                print("in handler: {}".format(self.get()))
                rand = random.randint(2, 3)
                print("rand is {}".format(rand))
                self.var = rand
                print("in handler: {}".format(self.get()))
                time.sleep(10)

    p = Gossiper(1)
    p.start()
    p.join()
