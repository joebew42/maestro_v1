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
        self.__dependencies = self.__load_dependencies(self.__services, self.__json)

    def __load_services(self, json):
        services = {}
        for item in json:
            name = item['name']
            policy = item.get('restart', RestartPolicy.NONE)
            services[name] = Service(name, item['command'], policy)
        logging.info("JSONPARSER >> Resolved services: {}".format(services.values()))
        return services

    def __load_dependencies(self, services, json):
        dependencies = []
        for item in json:
            for dependency_name in item.get('requires', []):
                dependencies.append((services[item['name']], services[dependency_name]))
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

from multiprocessing import Queue

class Supervisor:
    def __init__(self, scheduler, logfile_name="supervisor.log"):
        self.__scheduler = scheduler
        self.__services = {}
        self.__logfile_name = logfile_name
        self.__logfile = None
        self.__queue = Queue()

    def add(self, service):
        self.__scheduler.add(service)

    def add_dependency(self, service, required_service):
        self.__scheduler.add_dependency(service, required_service)

    def start(self):
        self.__logfile = open(self.__logfile_name, "a")

        for service in self.__scheduler.sorted_services():
            self.__services[service.name()] = service
            self.__spawn(service, self.__logfile)

        logging.info("SUPERVISOR >> Start monitoring")
        try:
            self.__run()
        except KeyboardInterrupt:
            logging.info("SUPERVISOR >> Keyboard Interrupt received: Stopping ...")
        finally:
            self.__logfile.close()

    def __run(self):
        while(True):
            msg = self.__queue.get(True, None)
            logging.info("SUPERVISOR << Received message [{0}]".format(msg))

            service = self.__services[msg['service_name']]
            returncode = msg['service_returncode']

            if returncode is not None:
                if service.policy() == RestartPolicy.NONE:
                    self.__remove(service)

                if service.policy() == RestartPolicy.ALWAYS:
                    self.__restart(service)

                if service.policy() == RestartPolicy.ON_ERROR and returncode != 0:
                    self.__restart(service)

    def __spawn(self, service, logfile):
        serviceprocess = ServiceProcess(service.name(), service.command(), self.__queue, logfile)
        serviceprocess.start()

    def __remove(self, service):
        logging.info("SUPERVISOR >> Removing [{0}] from services".format(service.name()))
        del self.__services[service.name()]

    def __restart(self, service):
        # TODO Restart Strategy http://www.erlang.org/doc/design_principles/sup_princ.html
        logging.info("SUPERVISOR >> Trying to restart [{0}]".format(service.name()))
        self.__spawn(service, self.__logfile)

# # # SERVICE PROCESS # # #

from multiprocessing import Process

class ServiceProcess(Process):
    def __init__(self, name, command, queue, logfile=None):
        super().__init__()
        self.__name =  name
        self.__command = command
        self.__queue = queue
        self.__logfile = logfile

    def run(self):
        process = subprocess.Popen(self.__command, shell=True, stdout=self.__logfile, stderr=self.__logfile)
        logging.info("SERVICEPROCESS >> Spawned [{0}]: PID [{1}]".format(self.__name, process.pid))
        try:
            process.wait()
        except KeyboardInterrupt:
            process.poll()
        finally:
            logging.info("SERVICEPROCESS >> [{0}] with PID [{1}] terminates with returncode [{2}]".format(self.__name, process.pid, process.returncode))
            self.__queue.put({'service_name' : self.__name, 'service_returncode' : process.returncode})

# # # RESTART POLICIES # # #

class RestartPolicy:
    NONE     = "none"
    ALWAYS   = "always"
    ON_ERROR = "on-error"

# # # SERVICE # # #

class Service:
    def __init__(self, name, command, policy=RestartPolicy.NONE):
        self.__name = name
        self.__command = command
        self.__policy = policy

    def name(self):
        return self.__name

    def command(self):
        return self.__command

    def policy(self):
        return self.__policy

    def __str__(self):
        return self.name()

    __repr__ = __str__

# # # MAIN # # #

import sys

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        filename = 'deploy.json'
    else:
        filename = sys.argv[1]

    scheduler = DAGScheduler()
    supervisor = Supervisor(scheduler)

    with open(filename, 'r') as json_file:
        parser = JSONParser(json_file.read())

    for service in parser.services():
        supervisor.add(service)

    for dependency in parser.dependencies():
        supervisor.add_dependency(dependency[0], dependency[1])

    supervisor.start()
