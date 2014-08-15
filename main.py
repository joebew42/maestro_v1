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
        self.__services = self.__load_services(self.__json)
        self.__dependencies = self.__load_dependencies(self.__services)

    def __load_services(self, json):
        services = {}
        for item in json:
            name = item['name']
            policy = item.get('restart', RestartPolicy.NONE)
            services[name] = Service(name, item['command'], policy, item.get('requires', []))
        logging.info("JSONPARSER >> Resolved services: {}".format(services.values()))
        return services

    def __load_dependencies(self, services):
        dependencies = []
        for service in services.values():
            for dependency_name in service.dependencies():
                dependencies.append((service, services[dependency_name]))
        logging.info("JSONPARSER >> Resolved dependencies: {}".format(dependencies))
        return dependencies

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

    def __init__(self):
        self.__services = nx.DiGraph()

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
                service.ping()
                sleep(1)

                if service.returncode() is not None:
                    if service.policy() == RestartPolicy.NONE:
                        self.__remove(service)

                    if service.policy() == RestartPolicy.ALWAYS:
                        self.__restart(service)

                    if service.policy() == RestartPolicy.ON_ERROR and service.returncode() != 0:
                        self.__restart(service)

    def __init(self):
        self.__logfile = open(self.__logfile_name, "a")
        self.__services = self.__scheduler.sorted_services()
        for service in self.__services:
            service.start(self.__logfile)

    def __remove(self, service):
        logging.info("SUPERVISOR >> [{0}] with PID [{1}] terminates with returncode [0]".format(service.name(), service.pid()))
        self.__services.remove(service)

    def __restart(self, service):
        # TODO implement a restart strategy
        # Reference: http://www.erlang.org/doc/design_principles/sup_princ.html
        logging.info("SUPERVISOR >> Terminating... Trying to restart [{0}] with PID [{1}]".format(service.name(), service.pid()))
        service.start(self.__logfile)

    def stop(self):
        self.__logfile.close()
        for service in reversed(self.__services):
            service.stop()

# # # RESTART POLICIES # # #

class RestartPolicy:
    NONE     = "none"
    ALWAYS   = "always"
    ON_ERROR = "on-error"

# # # SERVICE # # #

class Service:
    def __init__(self, name, command, policy=RestartPolicy.NONE, dependencies=[]):
        self.__name = name
        self.__command = command
        self.__process = None
        self.__policy = policy
        self.__dependencies = dependencies

    def name(self):
        return self.__name

    def command(self):
        return self.__command

    def policy(self):
        return self.__policy

    def dependencies(self):
        return self.__dependencies

    def pid(self):
        return self.__process.pid

    def returncode(self):
        return self.__process.returncode

    def start(self, logfile=None):
        # TODO if self.__can_run
        self.__process = subprocess.Popen(self.__command, shell=True, stdout=logfile, stderr=logfile)
        logging.info("SERVICE >> Spawned [{0}]: PID [{1}] and Restart Policy [{2}]".format(self.name(), self.pid(), self.policy()))

    def stop(self):
        try:
            self.__process.terminate()
            self.__process.poll()
            logging.info("SERVICE >> Killed [{0}] with PID [{1}] returned with [{2}]".format(self.name(), self.pid(), self.returncode()))
        except Exception as exception:
            logging.info("SERVICE >> Unable to terminate [{0}] with PID [{1}]. Reason: {2}".format(self.name(), self.pid(), exception))

    def ping(self):
        self.__process.poll()
        logging.info("SERVICE << Ping: [{0}] with PID [{1}] has a returncode [{2}]".format(self.name(), self.pid(), self.returncode()))

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
