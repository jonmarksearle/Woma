import sys
import time
import json
from itertools import chain
from dataclasses import dataclass
from typing import Callable, Iterator, Union, Optional, List


def get_function_name():
    return str(sys._getframe(1).f_code.co_name)


# def func_line():
#     return f'{str(sys._getframe(up).f_code.co_filename)}:{str(sys._getframe(up).f_code.co_firstlineno)}''


def log(*args, **kwargs):
    """ log
        print the arguments to stdout as json if possible otherwise just use the 
    """
    def log_obj(obj):
        try:
            print(json.dumps(obj))
        except TypeError:
            print(obj)

    for obj in args:
        log_obj(obj)
    if kwargs is not None and len(kwargs)>0:
        log_obj(kwargs)

def pplog(*args, **kwargs):
    """ pretty print log
        print the arguments to stdout as json if possible otherwise just use the 
    """
    def log_obj(obj):
        try:
            print(json.dumps(obj, indent = 4))
        except TypeError:
            print(obj)

    for obj in args:
        log_obj(obj)
    if kwargs is not None and len(kwargs)>0:
        log_obj(kwargs)

# ## Original LogTime class ######################################

# class LogTimes:

#     def __init__(self, log_name):
#         self._logs = []
#         self._thislog = None
#         self._total_time = 0
#         self.log_name = log_name

#     def start(self, description):
#         self._thislog = dict(starttime = time.time(), description = description )

#     def stop(self):
#         assert self._thislog is not None, ''
#         self._thislog['stoptime'] = time.time()
#         self._thislog['runtime'] = self._thislog['stoptime'] - self._thislog['starttime']
#         self._total_time += self._thislog['runtime']
#         self._logs.append(self._thislog)
#         self._thislog = None

#     def number_of_logs(self):
#         return len(self._logs)

#     def total_seconds(self):
#         return self._total_time

#     def print_summary(self):
#         pplog(
#             log_name = self.log_name,
#             total_time = self._total_time, 
#             number = self.number_of_logs()
#             )

#     def print_logs(self):
#         pplog(self._logs)

#     def __del__(self):
#         self.print_summary()
#         # self.print_logs()



## ######## Timer class ######## ##

class Timer:
    def __init__(self, timer_name:str):
        self._timer_name : str = timer_name
        self._start_time : float = None
        self._number_of_starts : int = 0
        self._run_time : float = 0.0

    @property
    def name(self) -> str:
        return self._timer_name

    def _start(self, start_time : float):
        assert self.stopped, f'timer [{self._timer_name}] has already started'
        self._start_time = start_time
        self._number_of_starts += 1
        # print(f'starting timer  {self._timer_name}' )

    def start(self):
        self._start(time.time())
        # time.sleep(0.0000000000000001)  ## not sure why but time.time wants at least few nano-seconds between calls
        return self

    @property
    def started(self):
        return self._start_time is not None

    @property
    def start_time(self) -> float:
        return self._start_time

    @property
    def number_of_starts(self) -> int:
        return self._number_of_starts

    def _stop(self, stop_time : float):
        assert self.started, f'timer [{self._timer_name}] has not started, or has already stopped'
        self._run_time += stop_time - self._start_time
        self._start_time = None

    def stop(self):
        self._stop(time.time())
        return self

    @property
    def stopped(self) -> bool:
        return not self.started

    @property
    def seconds(self) -> float:
        if self.started:
            return self._run_time + time.time() - self.start_time
        else:
            return self._run_time

    @property
    def report(self) -> dict:
        return dict(
            name = self.name,
            status = 'started' if self.started else 'stopped',
            number_of_starts = self.number_of_starts,
            seconds = self.seconds,
        )

    def __str__(self) -> str:
        if self.started:
            return f"{type(self).__name__}: '{self.name}', started at {self.start_time}, ran for {self.seconds}s, started {self.number_of_starts} time(s)"
        else:
            return f"{type(self).__name__}: '{self.name}', ran for {self.seconds}s, started {self.number_of_starts} time(s)"


# class time_this()
    # context manager: aka, with clause
    # def __init__(self, timer_name=''):
    #    self._timer = Timer(timer_name)
    #
    # def __enter__(self):
    #   self._timer.start()
    #   return self._timer
    #
    # def __exit__(self, *exec):
    #   return False


## ######## TimerCollection ######## ##

class TimerCollection:
    def __init__(self, collection_name : str):
        self._collection_name : str = collection_name
        self._collection : dict[Timer] = dict()

    def _new_timer(self, timer_name : str) -> Timer:
        return Timer(timer_name)

    def _add_timer(self, timer_name : str):
        assert not timer_name in self._collection, f'timer [{timer_name}] already exists'
        self._collection[timer_name] = self._new_timer(timer_name)

    def get_timer(self, timer_name : str) -> Timer:
        assert timer_name in self._collection, f'timer [{timer_name}] does not exist'
        return self._collection[timer_name]

    def start(self, timer_name : str):
        if not timer_name in self._collection:
            self._add_timer(timer_name)
        self.get_timer(timer_name).start()

    def started(self, timer_name : str) -> bool:
        return self.get_timer(timer_name).started

    def stopped(self, timer_name : str) -> bool:
        return self.get_timer(timer_name).stopped

    def stop(self, timer_name : str):
        self.get_timer(timer_name).stop()

    def seconds(self, timer_name : str) -> float:
        return self.get_timer(timer_name).seconds

    def number_of_starts(self, timer_name : str) -> int:
        return self.get_timer(timer_name).number_of_starts

    @property
    def number_of_timers(self) -> int:
        return len(self._collection)

    @property
    def report(self) -> List[dict]:
        return [ timer.report for _, timer in self._collection.items() ]

    def __str__(self) -> str:
        return '\n'.join( chain(
            [ f'{type(self).__name__}: {self._collection_name}' ],
            [ str(timer) for _, timer in self._collection.items() ]
        ))


## ######## LogTimer ######## ##

@dataclass
class TimeLog:
    __slots__ =['start_time','stop_time']
    start_time : float
    stop_time : float

    @property
    def seconds(self) -> float:
        return self.stop_time - self.start_time

    def __str__(self) -> str:
        return f'{type(self).__name__}(start_time={self.start_time}, stop_time={self.stop_time}, seconds={self.seconds})'


class LogTimer(Timer):
    def __init__(self, timer_name : str):
        super().__init__(timer_name)
        self._logs : List[TimeLog] = []

    def _add_time_log(self, start_time:float, stop_time:float):
        self._logs.append(TimeLog(
            start_time = start_time,
            stop_time = stop_time,
        ))

    def stop(self):
        stop_time = time.time()
        self._add_time_log(self.start_time, stop_time)
        self._stop(stop_time)

    @property
    def number_of_logs(self) -> int:
        return self.number_of_starts

    @property
    def total_seconds(self) -> float:
        return self.seconds

    @property
    def logs(self) -> List[TimeLog]:
        return self._logs


# ######## LogTimerCollection ######## ##

class LogTimerCollection(TimerCollection):

    def _new_timer(self, timer_name : str) -> Timer:
        return LogTimer(timer_name)

    def number_of_logs(self, timer_name):
        return self.get_timer(timer_name).number_of_logs

    def get_log_timer(self, timer_name):
        return self.get_timer(timer_name)

    def get_timer_logs(self, timer_name):
        return self.get_timer(timer_name).logs

    @property
    def summary_report(self):
        return [
            dict(
                timer_name = timer_name,
                timer_log_count = timer_log.number_of_logs,
                timer_seconds = timer_log.total_seconds,
            )
            for timer_name, timer_log in self._collection.items()
        ]

    def log_summary_report(self):
        pplog(
            timer_collection = self._collection_name,
            summary_report = self.summary_report
            )
