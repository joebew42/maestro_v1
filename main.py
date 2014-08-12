#!/usr/bin/env python

import logging

# # # JSON PARSER # # #

import json

class JSONParser:
    """
    It's read a JSON object and returns services and dependencies
    """

    def __init__(self, json):
        self.__json = json

    def services(self):
        return []

    def dependencies(self):
        return []

# # # SCHEDULER # # #

import networkx as nx

class DAGScheduler:
    """
    This is a scheduler based on a Directed Acyclic Graph
    """

    __services = nx.DiGraph()

    def add(self, service):
        self.__services.add_node(service.name(), service=service)

    def add_dependency(self, service, required_service):
        self.__services.add_edge(service.name(), required_service.name())

    def sorted_services(self):
        sorted_services = [self.__services.node[name]['service'] for name in nx.topological_sort(self.__services, reverse=True)]
        logging.info("SCHEDULER >> Computed topological sorting of the services is: {}".format(sorted_services))
        return sorted_services

# # # SUPERVISOR # # #

import subprocess

from threading import Thread
from time import sleep

class Supervisor(Thread):
    def __init__(self, scheduler):
        super().__init__()
        self.__scheduler = scheduler
        self.__services = []

    def add(self, service):
        self.__scheduler.add(service)

    def add_dependency(self, service, required_service):
        self.__scheduler.add_dependency(service, required_service)

    def run(self):
        self.__init()
        logging.info("SUPERVISOR >> Start monitoring")
        while(True):
            for service in self.__services:
                self.__ping(service)

                if service.returncode() == 0:
                     logging.info("SUPERVISOR >> [{0}] with PID [{1}] terminates with returncode [0]".format(service.name(), service.pid()))
                     self.__services.remove(service)

                if service.returncode() == 1:
                    logging.info("SUPERVISOR >> Terminating... Trying to restart [{0}] with PID [{1}]".format(service.name(), service.pid()))
                    self.__restart(service)

    def __init(self):
        self.__services = self.__scheduler.sorted_services()
        for service in self.__services:
            # TODO if can_run(service) ...
            self.__spawn_process(service)

    def __restart(self, service):
        # TODO implement a restart strategy
        # Reference: http://www.erlang.org/doc/design_principles/sup_princ.html
        logging.info("SUPERVISOR >> Restarting [{0}]".format(service.name()))
        self.__spawn_process(service)

    def __spawn_process(self, service):
        process = subprocess.Popen(service.command(), shell=True)
        service.set_process(process)
        logging.info("SUPERVISOR >> Spawned [{0}] with PID [{1}]".format(service.name(), service.pid()))

    def __ping(self, service):
        logging.info("SUPERVISOR >> Ping: [{0}] with PID [{1}]".format(service.name(), service.pid()))
        service.poll()
        sleep(1)
        logging.info("SUPERVISOR << Pong: [{0}] with PID [{1}] has a returncode [{2}]".format(service.name(), service.pid(), service.returncode()))

# # # SERVICE # # #

class Service:
    def __init__(self, name, command):
        self.__name = name
        self.__command = command
        self.__process = None

    def name(self):
        return self.__name

    def command(self):
        return self.__command

    def set_process(self, process):
       self.__process = process

    def poll(self):
        self.__process.poll()

    def pid(self):
        return self.__process.pid

    def returncode(self):
        return self.__process.returncode

    def __str__(self):
        return self.name()

    __repr__ = __str__

# # # MAIN # # #

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    scheduler = DAGScheduler()
    supervisor = Supervisor(scheduler)

    data = Service('data', 'sleep 60; exit 1')
    mysql = Service('mysql', 'sleep 50; exit 1')
    redis = Service('redis', 'sleep 40; exit 1')
    app = Service('app', 'sleep 30; exit 1')
    nginx = Service('nginx', 'sleep 20; exit 1')
    batch = Service('batch', 'sleep 4; exit 0')

    supervisor.add(data)
    supervisor.add(mysql)
    supervisor.add(redis)
    supervisor.add(app)
    supervisor.add(nginx)
    supervisor.add(batch)

    supervisor.add_dependency(mysql, data)
    supervisor.add_dependency(app, mysql)
    supervisor.add_dependency(app, redis)

    supervisor.start()


# JSON EXAMPLE
# [
#     {
#         "data": {
#             "command": "sleep 60; exit 1"
#         }
#     },
#     {
#         "mysql": {
#             "command": "sleep 50; exit 1",
#             "requires": [
#                 "data"
#             ]
#         }
#     },
#     {
#         "redis": {
#             "command": "sleep 40; exit 1"
#         }
#     },
#     {
#         "app": {
#             "command": "sleep 30; exit 1",
#             "requires": [
#                 "mysql",
#                 "redis"
#             ]
#         }
#     },
#     {
#         "nginx": {
#             "command": "sleep 20; exit 1"
#         }
#     }
# ]
