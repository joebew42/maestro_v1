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

# # # INIT PROCESS # # #

import subprocess

class InitProcess:
    def __init__(self, scheduler):
        self.__scheduler = scheduler
        self.__probes = []

    def add(self, service):
        self.__scheduler.add(service)

    def add_dependency(self, service, required_service):
        self.__scheduler.add_dependency(service, required_service)

    def start(self):
        for service in self.__scheduler.sorted_services():
            # TODO if can_run(service) ...
            self.__start(service)

    def restart(self, service):
        # TODO implement a restart strategy
        # Reference: http://www.erlang.org/doc/design_principles/sup_princ.html
        logging.info("INIT >> Restarting [{0}]".format(service.name()))
        self.__start(service)

    def __start(self, service):
        self.__spawn_process(service)
        self.__attach_probe(service)

    def __spawn_process(self, service):
        process = subprocess.Popen(service.command(), shell=True)
        service.set_process(process)
        logging.info("INIT >> Spawned [{0}] with PID [{1}]".format(service.name(), service.pid()))

    def __attach_probe(self, service):
        probe = Probe(service, self)
        probe.start()

# # # PROBE # # #

from threading import Thread
from time import sleep

class Probe(Thread):
    def __init__(self, service, init_process):
        super().__init__()
        self.__service = service
        self.__init = init_process

    def run(self):
        logging.info("PROBE >> Starting for [{0}] with PID [{1}]".format(self.__service.name(), self.__service.pid()))
        while(True):
            self.__heartbeat()

            if self.__is_terminated():
                logging.info("PROBE >> Terminating for [{0}] with PID [{1}]".format(self.__service.name(), self.__service.pid()))
                return

            if self.__is_to_respawn():
                logging.info("PROBE >> Terminating... Trying to restart [{0}] with PID [{1}]".format(self.__service.name(), self.__service.pid()))
                self.__init.restart(self.__service)
                return

            sleep(1)

    def __heartbeat(self):
        logging.info("PROBE >> Ping: heartbeat for [{0}] with PID [{1}]".format(self.__service.name(), self.__service.pid()))
        self.__service.poll()
        logging.info("PROBE << Pong: [{0}] with PID [{1}] has a returncode [{2}]".format(self.__service.name(), self.__service.pid(), self.__service.returncode()))

    def __is_terminated(self):
        return self.__service.returncode() == 0

    def __is_to_respawn(self):
        return self.__service.returncode() == 1

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
    init_process = InitProcess(scheduler)

    data = Service('data', 'sleep 60; exit 1')
    mysql = Service('mysql', 'sleep 50; exit 1')
    redis = Service('redis', 'sleep 40; exit 1')
    app = Service('app', 'sleep 30; exit 1')
    nginx = Service('nginx', 'sleep 20; exit 1')

    init_process.add(data)
    init_process.add(mysql)
    init_process.add(redis)
    init_process.add(app)
    init_process.add(nginx)

    init_process.add_dependency(mysql, data)
    init_process.add_dependency(app, mysql)
    init_process.add_dependency(app, redis)

    init_process.start()
