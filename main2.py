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
from os import killpg, setsid
from signal import SIGTERM
from shlex import split as shell_split

class OSProcessMonitorThread(Thread):
    def __init__(self, service, osprocess_request_queue):
        super().__init__()
        self._service = service
        self._osprocess_request_queue = osprocess_request_queue
        self._response_queue = Queue()
        self._process = None
        self._notify = True

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

    def terminate(self):
        self._notify = False
        killpg(self._process.pid, SIGTERM)

    def get_response(self):
        return self._response_queue.get()

    def _wait_until_started(self):
        while not self._has_started():
            sleep(1)
        self._response_queue.put((OSProcessMonitorThreadMessage.STARTED, self._process.pid))

    def _wait_until_stopped(self):
        while not self._has_stopped():
            sleep(1)
        self._response_queue.put((OSProcessMonitorThreadMessage.STOPPED, self._process.pid, self._process.returncode))

        if self._notify == True and self._service.is_to_be_restart_with(self._process.returncode):
            self._osprocess_request_queue.put((OSProcessThreadMessage.RESTART,))

    def _has_started(self):
        return True

    def _has_stopped(self):
        return True

    def _spawn_process(self):
        _cmd = "sh -c \"{}\"".format(self._service.command().replace('"', '\\"'))
        _args = shell_split(_cmd)
        return Popen(_args, shell=False, preexec_fn=setsid, stdout=None, stderr=None)

    def _post_exec(self):
        pass


# # # OS PROCESS THREAD MESSAGE # # #

class OSProcessThreadMessage:
    START = "START"
    STOP = "STOP"
    RESTART = "RESTART"


# # # OS PROCESS THREAD # # #

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
            self.__osprocess_monitor_thread.terminate()

            logging.info("{}:{} >> Response from [{}]: {}".format(
                self.__class__.__name__,
                self.__service,
                self.__osprocess_monitor_thread.__class__.__name__,
                self.__osprocess_monitor_thread.get_response()
            ))


# # # SERVICE THREAD MESSAGE # # #

class ServiceThreadMessage:
    RESTART = "RESTART"
    ADD_CHILD = "ADD_CHILD"


# # # SERVICE THREAD # # #

class ServiceThread(Thread):
    def __init__(self, service):
        super().__init__()
        self.__service = service
        self.__request_queue = Queue()
        self.__children = []
        self.__handlers = {
            ServiceThreadMessage.RESTART : self.__restart,
            ServiceThreadMessage.ADD_CHILD : self.__add_child,
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

    def put_request(self, message, block=False):
        self.__request_queue.put(message)
        if block:
            self.__request_queue.join()

    def __restart(self, message):
        logging.info("{}:{} << Received RESTART message".format(self.__class__.__name__, self.__service))

        self.__send_to_osprocess_thread((OSProcessThreadMessage.STOP,))
        self.__send_to_osprocess_thread((OSProcessThreadMessage.START,))

        self.__notify((ServiceThreadMessage.RESTART,))

    def __send_to_osprocess_thread(self, message):
        self.__osprocess_thread.put_request(message)
        _process_response = self.__osprocess_thread.get_response()
        logging.info("{}:{} << Received: {}".format(self.__class__.__name__, self.__service, _process_response))

    def __add_child(self, message):
        _child = message[0]
        self.__children.append(_child)
        logging.info("{} >> Registered to publish over [{}]".format(self, _child))

    def __notify(self, message):
        for child in self.__children:
            child.put_request(message)

    def __str__(self):
        return "{}:{}".format(self.__class__.__name__, self.__service)


# # # SUPERVISOR # # #

import networkx as nx

class Supervisor:
    def __init__(self):
        self.__services = {}
        self.__graph = nx.DiGraph()

    def add(self, service):
        self.__graph.add_node(service)

    def add_dependency(self, required_service, service):
        self.__graph.add_edge(required_service, service)

    def init(self):
        for service in self.__graph.nodes():
            self.__services[service] = ServiceThread(service)
            self.__services[service].start()

        for edge in self.__graph.edges():
            _parent_service, _child_service = edge

            self.__services[_parent_service].put_request((ServiceThreadMessage.ADD_CHILD, self.__services[_child_service]), True)

    def start(self):
        for service in self.__graph.nodes():
            if len(self.__graph.in_edges(service)) == 0:
                self.__services[service].put_request((ServiceThreadMessage.RESTART,))


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

    def is_to_be_restart_with(self, returncode):
        if self.__policy == RestartPolicy.ALWAYS:
            return True

        if self.__policy == RestartPolicy.ON_ERROR and returncode != 0:
            return True

        return False

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
