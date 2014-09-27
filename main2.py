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


# # # OS PROCESS MONITOR THREAD MESSAGE # # #

class OSProcessMonitorThreadMessage:
    STARTED = "STARTED"
    STOPPED = "STOPPED"

# # # OS PROCESS MONITOR THREAD # # #

from queue import Queue
from threading import Thread
from subprocess import Popen
from time import sleep
from os import kill

class OSProcessMonitorThread(Thread):
    def __init__(self, service, osprocess_request_queue):
        super().__init__()
        self._service = service
        self._osprocess_request_queue = osprocess_request_queue
        self._response_queue = Queue()
        self._process = None
        self._safe_kill = False

    def run(self):
        self._process = self._spawn_process()

        logging.info("{}:{} >> Spawned [{}] with restart policy [{}]".format(
            self.__class__.__name__,
            self._service,
            self._process.pid,
            self._service.policy()
        ))

        try:
            self._wait_until_started()
            self._process.wait()
        except KeyboardInterrupt:
            self._process.poll()
        finally:
            self._post_exec()
            self._wait_until_stopped()

            logging.info("{}:{} >> PID [{}] has exited with [{}]".format(
                self.__class__.__name__,
                self._service,
                self._process.pid,
                self._process.returncode
            ))

    def kill(self):
        self._safe_kill = True
        try:
            kill(self._process.pid, SIGTERM)
        except ProcessLookupError:
            pass

    def get_response(self):
        return self._response_queue.get()

    def _wait_until_started(self):
        while not self._has_started():
            sleep(1)
        self._response_queue.put((OSProcessMonitorThreadMessage.STARTED, self._process.pid))

    def _wait_until_stopped(self):
        while not self._has_stopped():
            sleep(1)
        self._response_queue.put((OSProcessMonitorThreadMessage.STOPPED, self._process.pid))

        if self._safe_kill == False and self._service.is_always_restart():
            self._osprocess_request_queue.put((OSProcessThreadMessage.RESTART,))

    def _has_started(self):
        return True

    def _has_stopped(self):
        return True

    def _spawn_process(self):
        return Popen(self._service.command(), shell=True, stdout=None, stderr=None)

    def _post_exec(self):
        pass


# # # OS PROCESS THREAD MESSAGE # # #

class OSProcessThreadMessage:
    START = "START"
    STOP = "STOP"
    RESTART = "RESTART"


# # # OS PROCESS THREAD # # #

from signal import SIGTERM

class OSProcessThread(Thread):
    def __init__(self, service, service_request_queue):
        super().__init__()
        self.__service = service
        self.__service_request_queue = service_request_queue
        self.__request_queue = Queue()
        self.__response_queue = Queue()
        self.__handlers = {
            OSProcessThreadMessage.START : self.__start,
            OSProcessThreadMessage.STOP : self.__stop,
            OSProcessThreadMessage.RESTART : self.__restart,
        }
        self.__osprocess_monitor_thread = Thread()

    def run(self):
        while True:
            _message = self.__request_queue.get()
            self.__handlers.get(_message[0])()
            self.__request_queue.task_done()

    def put_request(self, message):
        self.__request_queue.put(message)
        self.__request_queue.join()

    def get_response(self):
        return self.__response_queue.get()

    def __start(self):
        logging.info("{}:{} << Received: [{}]".format(self.__class__.__name__, self.__service, OSProcessThreadMessage.START))

        self.__start_osprocess_monitor_thread()

        self.__response_queue.put((OSProcessMonitorThreadMessage.STARTED,))

    def __stop(self):
        logging.info("{}:{} << Received: [{}]".format(self.__class__.__name__, self.__service, OSProcessThreadMessage.STOP))

        self.__stop_osprocess_monitor_thread()

        self.__response_queue.put((OSProcessMonitorThreadMessage.STOPPED,))

    def __restart(self):
        logging.info("{}:{} << Received: [{}]".format(self.__class__.__name__, self.__service, OSProcessThreadMessage.RESTART))

        self.__service_request_queue.put((ServiceThreadMessage.RESTART,))

    def __start_osprocess_monitor_thread(self):
        self.__osprocess_monitor_thread = OSProcessMonitorThread(self.__service, self.__request_queue)
        self.__osprocess_monitor_thread.start()

        logging.info("{}:{} >> Response from [{}]: {}".format(
            self.__class__.__name__,
            self.__service,
            self.__osprocess_monitor_thread.__class__.__name__,
            self.__osprocess_monitor_thread.get_response()
        ))

    def __stop_osprocess_monitor_thread(self):
        if self.__osprocess_monitor_thread.is_alive():
            self.__osprocess_monitor_thread.kill()

            logging.info("{}:{} >> Response from [{}]: {}".format(
                self.__class__.__name__,
                self.__service,
                self.__osprocess_monitor_thread.__class__.__name__,
                self.__osprocess_monitor_thread.get_response()
            ))

# # # SERVICE THREAD MESSAGE # # #

class ServiceThreadMessage:
    RESTART = "RESTART"
    ADD_QUEUE = "ADD_QUEUE"


# # # SERVICE THREAD # # #

class ServiceThread(Thread):
    def __init__(self, service, queue):
        super().__init__()
        self.__service = service
        self.__request_queue = queue
        self.__response_queues = []
        self.__handlers = {
            ServiceThreadMessage.RESTART : self.__restart,
            ServiceThreadMessage.ADD_QUEUE : self.__add_queue,
        }
        self.__osprocess_thread = None

    def run(self):
        logging.info("{}:{} >> Ready".format(self.__class__.__name__, self.__service))

        self.__osprocess_thread = OSProcessThread(self.__service, self.__request_queue)
        self.__osprocess_thread.start()

        while True:
            _message = self.__request_queue.get()
            self.__handlers.get(_message[0])(_message[1:])
            self.__request_queue.task_done()

        self.__shutdown()

    def __shutdown(self):
        logging.info("{}:{} >> Shutting down".format(self.__class__.__name__, self.__service))

    def request_queue(self):
        return self.__request_queue

    def __restart(self, message):
        logging.info("{}:{} << Received RESTART message".format(self.__class__.__name__, self.__service))

        self.__send_to_osprocess_thread((OSProcessThreadMessage.STOP,))
        self.__send_to_osprocess_thread((OSProcessThreadMessage.START,))

        self.__notify((ServiceThreadMessage.RESTART,))

    def __send_to_osprocess_thread(self, message):
        self.__osprocess_thread.put_request(message)
        _process_response = self.__osprocess_thread.get_response()
        logging.info("{}:{} << Received: {}".format(self.__class__.__name__, self.__service, _process_response))

    def __add_queue(self, message):
        _service_name, _queue = message
        self.__response_queues.append(_queue)
        logging.info("{}:{} >> Registered to publish over [{}]".format(self.__class__.__name__, self.__service, _service_name))

    def __notify(self, message):
        for queue in self.__response_queues:
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
            _service_thread = self.__create_service_thread_for(service)
            _service_thread.start()

        for edge in self.__graph.edges():
            _parent_service, _child_service = edge

            self.__send_to(_parent_service, (ServiceThreadMessage.ADD_QUEUE, _child_service.name(), self.__queues[_child_service]), True)

    def start(self):
        for service in self.__graph.nodes():
            if len(self.__graph.in_edges(service)) == 0:
                self.__send_to(service, (ServiceThreadMessage.RESTART,))

    def __create_service_thread_for(self, service):
        _queue = self.__create_queue_for(service)
        return ServiceThread(service, _queue)

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

    def name(self):
        return self.__name

    def command(self):
        return self.__command

    def policy(self):
        return self.__policy

    def is_always_restart(self):
        return self.__policy == RestartPolicy.ALWAYS

    def provider(self):
        return self.__provider

    def params(self, index):
        return self.__params.get(index, [])

    def param(self, index):
        return self.__params.get(index, None)

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
