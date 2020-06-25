import time
import math

class Timer:
    timers = []

    def __init__(self, name):
        print (name + " (done)")
        Timer.timers.append((name, time.time()))


    def print_timings(self):

        timers = Timer.timers
        print ("{0} : {1} ".format("Start time", timers[0][1]))
        for idx in range(1,len(timers)):
            print (" {0} : {1}".format(timers[idx][0], timers[idx][1] -
                                                      timers[idx-1][1]))

        print ("{0} : {1} Total time".format("total time", timers[-1][1] -
                                            timers[0][1]))

class MultiTimer:
    timers = {}
    count = {}
    order = []
    last = None
    def __init__(self, name):
        current = time.time()
        if name in MultiTimer.timers:         
            MultiTimer.timers[name] = MultiTimer.timers[name] + \
                (current - MultiTimer.last )
            MultiTimer.count[name] += 1
        else:
            if not MultiTimer.last:
               MultiTimer.last = current
            MultiTimer.order.append(name)
            MultiTimer.timers[name] = 0.0 + (current - MultiTimer.last)
            MultiTimer.count[name] = 1
        MultiTimer.last = current
                            


    def print_timings(self):

        timers = MultiTimer.timers
        count = MultiTimer.count
        total = 0.0
        for key in MultiTimer.order:
            total += timers[key]

        print ("% : name : (count) : total time spend")
        for key in MultiTimer.order:
            if count[key] > 1:
                spaces = "  "
            else:
                spaces = ""
            print ("{0:>3} : {1} : {2} : {3} : {4}".format(
               int(round(100 * (timers[key] / total))),
               spaces , key, count[key], timers[key]))

        print ("Total time : {0}".format(total))

