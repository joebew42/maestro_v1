#!/usr/bin/env python

import logging

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

    def provider(self):
        return self.__provider

    def params(self, index):
        return self.__params.get(index, [])

    def param(self, index):
        return self.__params.get(index, None)

    def has_always_restart(self):
        return self.__policy == RestartPolicy.ALWAYS

    def has_on_error_restart(self):
        return self.__policy == RestartPolicy.ON_ERROR

    def __str__(self):
        return "{0}:{1}".format(self.__name, self.__provider)

    def __hash__(self):
        return hash(self.__name)

    __repr__ = __str__


# # # JSON PARSER # # #

import json

class JSONParser:
    """
    It reads a JSON text and returns services and dependencies
    """

    def __init__(self, json_text):
        self.__json = json.loads(json_text)
        self.__services = []
        self.__dependencies = []

    def parse(self):
        self.__services = self.__load_services(self.__json)
        self.__dependencies = self.__load_dependencies(self.__services, self.__json)

    def services(self):
        return self.__services.values()

    def dependencies(self):
        return self.__dependencies

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


# # # PROCESS THREAD MESSAGE # # #

class ProcessThreadMessage:
    STARTED = "STARTED"
    STOPPED = "STOPPED"


# # # PROCESS THREAD # # #

from queue import Queue
from threading import Thread
from time import sleep

class ProcessThread(Thread):
    def __init__(self, service_thread, logfile):
        super().__init__()
        self._service_thread = service_thread
        self._logfile = logfile
        self._service = service_thread.service()
        self._response_queue = Queue()
        self._process = None
        self._notify = True

    def run(self):
        self._spawn_process()

        logging.info("{} >> Spawned [{}] with restart policy [{}]".format(
            self,
            self._process_pid(),
            self._service.policy()
        ))

        try:
            self.__wait_until_started()
            self.__wait_until_exit()
        except KeyboardInterrupt:
            self._poll_process()
        finally:
            self._post_exec()
            self.__wait_until_stopped()

            logging.info("{} >> PID [{}] has exited with [{}]".format(
                self,
                self._process_pid(),
                self._process_returncode()
            ))

    def terminate(self):
        self._notify = False
        self._process_terminate()

    def get_response(self):
        return self._response_queue.get()

    def __wait_until_exit(self):
        while self._is_running():
            sleep(1)

    def __wait_until_started(self):
        while not self._has_started():
            sleep(1)

        self._response_queue.put((ProcessThreadMessage.STARTED, self._process_pid()))

    def __wait_until_stopped(self):
        while not self._has_stopped():
            sleep(1)

        self._response_queue.put((ProcessThreadMessage.STOPPED, self._process_pid(), self._process_returncode()))

        if self._notify == True and self.__is_to_be_restart_with(self._process_returncode()):
            self._service_thread.put_request((ServiceThreadMessage.RESTART,))

    def __is_to_be_restart_with(self, returncode):
        if self._service.has_always_restart():
            return True

        if self._service.has_on_error_restart() and returncode != 0:
            return True

        return False

    def _process_pid(self):
        return None

    def _process_poll(self):
        pass

    def _process_returncode(self):
        return None

    def _process_terminate(self):
        pass

    def _is_running(self):
        return True

    def _has_started(self):
        return True

    def _has_stopped(self):
        return True

    def _spawn_process(self):
        pass

    def _post_exec(self):
        pass

    def __str__(self):
        return "{}:{}".format(self.__class__.__name__, self._service)


# # # PROCESS COMMAD THREAD # # #

from os import killpg, setsid
from signal import SIGTERM
from subprocess import Popen
from shlex import split as shell_split

class ProcessCommandThread(ProcessThread):
    def _spawn_process(self):
        _cmd = "sh -c \"{}\"".format(self._service.command().replace('"', '\\"'))
        _args = shell_split(_cmd)
        self.__process = Popen(_args, shell=False, preexec_fn=setsid, stdout=self._logfile, stderr=self._logfile)

    def _is_running(self):
        self.__process.wait()

    def _process_terminate(self):
        killpg(self.__process.pid, SIGTERM)

    def _process_pid(self):
        return self.__process.pid

    def _process_poll(self):
        self.__process.poll()

    def _process_returncode(self):
        return self.__process.returncode

# # # PROCESS DOCKERFILE THREAD # # #

from subprocess import check_output

class ProcessDockerfileThread(ProcessThread):
    def _spawn_process(self):
        dockerfile_cmd = ["docker", "build", "-t", self._service.param('image'), self._service.param('path')]
        self.__process = Popen(dockerfile_cmd, shell=False, preexec_fn=setsid, stdout=self._logfile, stderr=self._logfile)

    def _has_started(self):
        _image = self._service.param('image').split(':')
        if len(_image) == 1:
            _image.append('latest')

        cmd = "docker images | awk '{{print $1$2}}' | grep \"{0}{1}\"".format(_image[0], _image[1])
        try:
            return len(check_output(cmd, shell=True)) > 0
        except:
            return False

    def _is_running(self):
        self.__process.wait()

    def _process_terminate(self):
        killpg(self.__process.pid, SIGTERM)

    def _process_pid(self):
        return self.__process.pid

    def _process_poll(self):
        self.__process.poll()

    def _process_returncode(self):
        return self.__process.returncode


# # # PROCESS DOCKER THREAD # # #

from os import getenv as os_getenv

from docker import Client as docker_client
from docker.errors import APIError as DockerAPIError

class ProcessDockerThread(ProcessThread):
    def _spawn_process(self):
        self.__docker = docker_client(base_url='unix://var/run/docker.sock')

        self.__cid = self.__docker.create_container(**self.__container_args())['Id']

        self.__docker.start(**self.__container_start_args())

        # TODO exception handling
        # try:
        #     self.__docker.start(**docker_start_args)
        # except DockerAPIError as error:
        #     logging.error("{} >> {}".format(
        #         self,
        #         error
        #     ))
        #     self.terminate()

    def _has_started(self):
        return self.__container_is_running()

    def _is_running(self):
        self.__returncode = self.__docker.wait(container=self.__cid)

    def _process_terminate(self):
        self.__docker.stop(container=self.__cid)

    def _post_exec(self):
        self.__docker.remove_container(container=self.__cid)

    def _has_stopped(self):
        return not self.__container_is_running()

    def _process_pid(self):
        return self.__cid

    def _process_returncode(self):
        return self.__returncode

    def __container_is_running(self):
        for container in self.__docker.containers(quiet=True):
            if self.__cid == container['Id']:
                return True
        return False

    def __container_args(self):
        command = self._service.param('command')
        command_args = []
        if command is not None:
            _cmd = "sh -c \"{}\"".format(command.replace('"', '\\"'))
            command_args = shell_split(_cmd)
            logging.info("{} >> Wants to run: {}".format(
                self,
                command_args
            ))

        _container_args = {
            'image' : self._service.param('image'),
            'command' : command_args,
            'detach' : True,
            'stdin_open' : True,
            'tty' : True,
            'ports' : [ int(port) for port in self._service.params('expose') ],
            'volumes' : [ volume.split(':')[1] for volume in self._service.params('volume') ],
            'environment' : [ self.__resolve_environment(variable) for variable in self._service.params('env') ],
            'name' : self._service.name()
        }

        return _container_args

    def __container_start_args(self):
        _container_start_args = {
            'container' : self.__cid,
            'binds' : { volume[0] : { 'bind' : volume[1], 'ro' : False } for volume in self.__volumes_bindings() },
            'volumes_from' : self._service.params('volumes_from'),
            'port_bindings' : { int(port[1]) : int(port[0]) for port in [ value.split(':') for value in self._service.params('port') ] }
            # TODO handle links
        }

        return _container_start_args

    def __volumes_bindings(self):
        return [ volume_binding.split(':') for volume_binding in self._service.params('volume') ]

    def __resolve_environment(self, variable):
        _variable, _value = env.split('=')
        return "{}={}".format(_variable, self.__read_environment_variable(_value))

    def __read_environment_variable(self, value):
        if value.startswith('$'):
            return os_getenv(value[1:])
        return value


# # # PROCESS THREAD FACTORY

class ProcessThreadFactory:
    PROCESS = {
        Provider.DOCKER : ProcessDockerThread,
        Provider.DOCKERFILE : ProcessDockerfileThread,
        Provider.DEFAULT : ProcessCommandThread,
    }

    @staticmethod
    def create(service_thread, logfile):
        _default = ProcessThreadFactory.PROCESS[Provider.DEFAULT]
        _service_provider = service_thread.service().provider()

        return ProcessThreadFactory.PROCESS.get(_service_provider, _default)(service_thread, logfile)


# # # SERVICE THREAD MESSAGE # # #

class ServiceThreadMessage:
    START = "START"
    STOP = "STOP"
    RESTART = "RESTART"
    HALT = "HALT"
    ADD_CHILD = "ADD_CHILD"
    ADD_DEPENDENCY = "ADD_DEPENDENCY"


# # # SERVICE THREAD # # #

from threading import Event

class ServiceThread(Thread):
    def __init__(self, service, logfile):
        super().__init__()
        self.__service = service
        self.__logfile = logfile
        self.__request_queue = Queue()
        self.__dependencies = []
        self.__children = []
        self.__handlers = {
            ServiceThreadMessage.START : self.__start,
            ServiceThreadMessage.STOP : self.__stop,
            ServiceThreadMessage.RESTART : self.__restart,
            ServiceThreadMessage.HALT : self.__halt,
            ServiceThreadMessage.ADD_CHILD : self.__add_child,
            ServiceThreadMessage.ADD_DEPENDENCY : self.__add_dependency,
        }
        self.__process_thread = Thread()
        self.__terminated = Event()
        # TODO: replace with state?
        self.__running = False

    def service(self):
        return self.__service

    def logfile(self):
        return self.__logfile

    def run(self):
        logging.info("{} >> Ready".format(self))

        while not self.__terminated.is_set():
            _message = self.__request_queue.get()
            self.__handlers.get(_message[0])(_message[1:])
            self.__request_queue.task_done()

    def put_request(self, message, block=False):
        self.__request_queue.put(message)
        if block:
            self.__request_queue.join()

    def is_running(self):
        return self.__running == True;

    def __start(self, message):
        logging.info("{} << Received START message".format(self))

        if not self.__dependencies_running():
            logging.info("{} << Unable to perform START: missing dependencies".format(self))
            return

        self.__process_thread = ProcessThreadFactory.create(self, self.__logfile)
        self.__process_thread.start()

        logging.info("{} >> Response from [{}]: {}".format(
            self,
            self.__process_thread,
            self.__process_thread.get_response()
        ))
        self.__running = True

        self.__notify((ServiceThreadMessage.RESTART,))

    def __dependencies_running(self):
        for dependency in self.__dependencies:
            if not dependency.is_running():
                logging.info("{} << [{}] is not yet started".format(self, dependency))
                return False
        return True

    def __stop(self, message):
        logging.info("{} << Received STOP message".format(self))

        if self.__process_thread.is_alive():
            self.__process_thread.terminate()

            logging.info("{} >> Response from [{}]: {}".format(
                self,
                self.__process_thread,
                self.__process_thread.get_response()
            ))
            self.__running = False

    def __restart(self, message):
        logging.info("{} << Received RESTART message".format(self))

        self.__stop(message)
        self.__start(message)

    def __halt(self, message):
        logging.info("{} << Received HALT message".format(self))

        self.__stop(message)
        self.__terminated.set()

    def __add_child(self, message):
        _child = message[0]
        self.__children.append(_child)
        logging.info("{} >> Registered to publish over [{}]".format(self, _child))

    def __add_dependency(self, message):
        _dependency = message[0]
        self.__dependencies.append(_dependency)
        logging.info("{} >> Registered dependency [{}]".format(self, _dependency))

    def __notify(self, message):
        for child in self.__children:
            child.put_request(message)

    def __str__(self):
        return "{}:{}".format(self.__class__.__name__, self.__service)


# # # SUPERVISOR # # #

import networkx as nx

from signal import signal

class Supervisor:
    def __init__(self, logfile_name="supervisor.log"):
        self.__services = {}
        self.__graph = nx.DiGraph()
        self.__logfile = open(logfile_name, "a")

    def add(self, service):
        self.__graph.add_node(service)
        self.__initialize_service(service)

    def add_dependency(self, required_service, service):
        self.__graph.add_edge(required_service, service)
        self.__initialize_dependency(required_service, service)

    def start(self):
        signal(SIGTERM, self.__shutdown)

        self.__boot()

        while True:
            try:
                sleep(10)
            except KeyboardInterrupt:
                self.__shutdown(None, None)

    def __initialize_service(self, service):
        self.__services[service] = ServiceThread(service, self.__logfile)
        self.__services[service].start()

    def __initialize_dependency(self, parent_service, child_service):
        self.__services[parent_service].put_request((ServiceThreadMessage.ADD_CHILD, self.__services[child_service]), True)
        self.__services[child_service].put_request((ServiceThreadMessage.ADD_DEPENDENCY, self.__services[parent_service]), True)

    def __boot(self):
        for service in self.__initial_services():
            self.__services[service].put_request((ServiceThreadMessage.START,))

    def __initial_services(self):
        return [service for service in self.__graph.nodes() if len(self.__graph.in_edges(service)) == 0]

    def __shutdown(self, signal_number, stack_frame):
        logging.info("{} >> Shutting down...".format(self))

        for service in self.__sorted_services()[::-1]:
            logging.info("{} >> Halting {} ...".format(self, service))
            self.__services[service].put_request((ServiceThreadMessage.HALT,), True)

        self.__logfile.close()
        exit(0)

    def __sorted_services(self):
        return nx.topological_sort(self.__graph)

    def __str__(self):
        return "{}".format(self.__class__.__name__)


# # # MAIN # # #

import sys

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    deploy_file = sys.argv[1]

    with open(deploy_file, 'r') as json_file:
        parser = JSONParser(json_file.read())
        parser.parse()

    supervisor = Supervisor()

    for service in parser.services():
        supervisor.add(service)

    for dependency in parser.dependencies():
        supervisor.add_dependency(dependency[0], dependency[1])

    supervisor.start()
