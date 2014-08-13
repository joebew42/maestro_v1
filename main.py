#!/usr/bin/env python

import logging

# # # JSON PARSER # # #

import json

class JSONParser:
    """
    It's read a JSON text and returns services and dependencies
    """

    def __init__(self, json_text):
        self.__json = json.loads(json_text)
        self.__services = {}
        self.__dependencies = []
        self.__parse()

    def __parse(self):
        self.__load_services()
        self.__load_dependencies()

    def __load_services(self):
        for item in self.__json:
            for name in item.keys():
                self.__services[name] = Service(name, item[name]['command'])
        logging.info("JSONPARSER >> Resolved services: {}".format(self.services()))

    def __load_dependencies(self):
        for item in self.__json:
            for name in item.keys():
                if 'requires' in item[name]:
                    for dependency in item[name]['requires']:
                        self.__dependencies.append((self.__services[name], self.__services[dependency]))
        logging.info("JSONPARSER >> Resolved dependencies: {}".format(self.dependencies()))

    def services(self):
        return self.__services.values()

    def dependencies(self):
        return self.__dependencies

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

from time import sleep

class Supervisor:
    def __init__(self, scheduler, logfile_name="supervisor.log"):
        self.__scheduler = scheduler
        self.__services = []
        self.__logfile_name = logfile_name
        self.__logfile = None

    def add(self, service):
        self.__scheduler.add(service)

    def add_dependency(self, service, required_service):
        self.__scheduler.add_dependency(service, required_service)

    def start(self):
        self.__init()
        logging.info("SUPERVISOR >> Start monitoring")
        while(True):
            for service in self.__services:
                self.__ping(service)

                if service.returncode() == 0:
                     logging.info("SUPERVISOR >> [{0}] with PID [{1}] terminates with returncode [0]".format(service.name(), service.pid()))
                     self.__services.remove(service)
                     continue

                if service.returncode() is not None:
                    logging.info("SUPERVISOR >> Terminating... Trying to restart [{0}] with PID [{1}]".format(service.name(), service.pid()))
                    self.__restart(service)

    def __init(self):
        self.__logfile = open(self.__logfile_name, "a")
        self.__services = self.__scheduler.sorted_services()
        for service in self.__services:
            self.__spawn_process(service)

    def __restart(self, service):
        # TODO implement a restart strategy
        # Reference: http://www.erlang.org/doc/design_principles/sup_princ.html
        logging.info("SUPERVISOR >> Restarting [{0}]".format(service.name()))
        self.__spawn_process(service)

    def __spawn_process(self, service):
        # TODO if can_run(service) ...
        process = subprocess.Popen(service.command(), shell=True, stdout=self.__logfile, stderr=self.__logfile)
        service.set_process(process)
        logging.info("SUPERVISOR >> Spawned [{0}] with PID [{1}]".format(service.name(), service.pid()))

    def __ping(self, service):
        logging.info("SUPERVISOR >> Ping: [{0}] with PID [{1}]".format(service.name(), service.pid()))
        service.poll()
        sleep(1)
        logging.info("SUPERVISOR << Pong: [{0}] with PID [{1}] has a returncode [{2}]".format(service.name(), service.pid(), service.returncode()))

    def stop(self):
        self.__logfile.close()
        for service in reversed(self.__services):
            self.__stop(service)

    def __stop(self, service):
        logging.info("SUPERVISOR >> Killing [{0}] with PID [{1}]".format(service.name(), service.pid()))
        service.stop()

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

    def stop(self):
        try:
            self.__process.terminate()
            self.poll()
            logging.info("SERVICE >> Killed [{0}] with PID [{1}] returned with [{2}]".format(self.name(), self.pid(), self.returncode()))
        except Exception as exception:
            logging.info("SERVICE >> Unable to terminate [{0}] with PID [{1}]. Reason: {2}".format(self.name(), self.pid(), exception))

    def __str__(self):
        return self.name()

    __repr__ = __str__

# # # MAIN # # #

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    scheduler = DAGScheduler()
    supervisor = Supervisor(scheduler)

    with open('deploy.json', 'r') as json_file:
        json_example = json_file.read()

    parser = JSONParser(json_example)

    for service in parser.services():
        supervisor.add(service)

    for dependency in parser.dependencies():
        supervisor.add_dependency(dependency[0], dependency[1])

    try:
        supervisor.start()
    except KeyboardInterrupt:
        supervisor.stop()
