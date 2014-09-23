#!/usr/bin/env python

import logging

# # # JSON PARSER # # #

import json

class JSONParser:
    """
    It reads a JSON text and returns services and dependencies
    """

    def __init__(self, json_text):
        self.__json = json.loads(json_text)
        self.__services = self.__load_services(self.__json)
        self.__dependencies = self.__load_dependencies(self.__services, self.__json)

    def __load_services(self, json):
        services = {}
        for item in json:
            name = item['name']
            command = item.get('command', '')
            policy = item.get('restart', RestartPolicy.NONE)
            provider = item.get('provider', Provider.DEFAULT)
            params = item.get('params', None)
            services[name] = Service(name, command, policy, provider, params)
        logging.info("JSONPARSER >> Resolved services: {}".format(services.values()))
        return services

    def __load_dependencies(self, services, json):
        dependencies = []
        for item in json:
            for dependency_name in item.get('requires', []):
                dependencies.append((services[dependency_name], services[item['name']]))
        logging.info("JSONPARSER >> Resolved dependencies: {}".format(dependencies))
        return dependencies

    def services(self):
        return self.__services.values()

    def dependencies(self):
        return self.__dependencies


# # # MONITOR MESSAGE # # #

class MonitorMessage:
    RESTART = 0
    ADD_QUEUE = 1

# # # MONITOR # # #

from time import sleep
from queue import Queue
from threading import Thread

class Monitor(Thread):
    def __init__(self, service, queue):
        super().__init__()
        self.__service = service
        self.__inbound_queue = queue
        self.__outbound_queues = []
        self.__handlers = {
            MonitorMessage.RESTART : self.__restart,
            MonitorMessage.ADD_QUEUE : self.__add_queue,
        }

    def run(self):
        logging.info("{}:{} >> Ready".format(self.__class__.__name__, self.__service))

        while True:
            message = self.__inbound_queue.get()
            self.__handlers.get(message[0], self.__null_handler)(message[1:])
            self.__inbound_queue.task_done()

        logging.info("{}:{} >> Shutting down".format(self.__class__.__name__, self.__service))

    def inbound_queue(self):
        return self.__inbound_queue

    def send(self, message):
        self.__inbound_queue.put(message)

    def __restart(self, message):
        logging.info("{}:{} << Received RESTART message".format(self.__class__.__name__, self.__service))
        sleep(5)
        self.__notify((MonitorMessage.RESTART,))

    def __add_queue(self, message):
        _service_name, _queue = message
        self.__outbound_queues.append(_queue)
        logging.info("{}:{} >> Registered to publish over [{}]".format(self.__class__.__name__, self.__service, _service_name))

    def __null_handler(self, message):
        logging.info("{}:{} << Received NULL message: {}".format(self.__class__.__name__, self.__service, message))

    def __notify(self, message):
        for queue in self.__outbound_queues:
            queue.put(message)

    def __str__(self):
        return "{}:{}".format(self.__class__.__name__, self.__service)


# # # SUPERVISOR # # #

import networkx as nx

class Supervisor:
    def __init__(self):
        self.__queues = {}
        self.__graph = nx.DiGraph()

    def add(self, service):
        self.__graph.add_node(service)

    def add_dependency(self, required_service, service):
        self.__graph.add_edge(required_service, service)

    def init(self):
        for service in self.__graph.nodes():
            _monitor = self.__create_monitor_for(service)
            _monitor.start()

        for edge in self.__graph.edges():
            _parent_service, _child_service = edge

            self.__send_to(_parent_service, (MonitorMessage.ADD_QUEUE, _child_service.name(), self.__queues[_child_service]), True)

    def start(self):
        for service in self.__graph.nodes():
            if len(self.__graph.in_edges(service)) == 0:
                self.__send_to(service, (MonitorMessage.RESTART,))

    def __create_monitor_for(self, service):
        _queue = self.__create_queue_for(service)
        return Monitor(service, _queue)

    def __create_queue_for(self, service):
        self.__queues[service] = Queue()
        return self.__queues[service]

    def __send_to(self, service, message, block=False):
        self.__queues[service].put(message)
        if block:
            self.__queues[service].join()


# # # RESTART POLICIES # # #

class RestartPolicy:
    NONE     = "none"
    ALWAYS   = "always"
    ON_ERROR = "on-error"


# # # PROVIDERS # # #

class Provider:
    DEFAULT    = "command"
    DOCKERFILE = "dockerfile"
    DOCKER     = "docker"


# # # SERVICE # # #

class Service:
    def __init__(self, name, command, policy=RestartPolicy.NONE, provider=Provider.DEFAULT, params=None):
        self.__name = name
        self.__command = command
        self.__policy = policy
        self.__provider = provider
        self.__params = params
        self.__pid = None

    def name(self):
        return self.__name

    def command(self):
        return self.__command

    def policy(self):
        return self.__policy

    def provider(self):
        return self.__provider

    def params(self, index):
        return self.__params.get(index, [])

    def param(self, index):
        return self.__params.get(index, None)

    def pid(self):
        return self.__pid

    def started(self, pid):
        self.__pid = pid

    def stopped(self):
        self.__pid = None

    def __str__(self):
        return "{0}:{1}".format(self.__name, self.__provider)

    def __hash__(self):
        return hash(self.__name)

    __repr__ = __str__

# # # MAIN # # #

import sys

from random import randrange

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        filename = 'conf.d/deploy.json'
    else:
        filename = sys.argv[1]

    with open(filename, 'r') as json_file:
        parser = JSONParser(json_file.read())

    supervisor = Supervisor()

    for service in parser.services():
        supervisor.add(service)

    for dependency in parser.dependencies():
        supervisor.add_dependency(dependency[0], dependency[1])

    supervisor.init()
    supervisor.start()
