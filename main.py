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


# # # OS PROCESS THREAD MESSAGE # # #

class OSProcessThreadMessage:
    STARTED = "STARTED"
    STOPPED = "STOPPED"


# # # OS PROCESS THREAD # # #

from queue import Queue
from threading import Thread
from time import sleep
from os import killpg, setsid
from signal import SIGTERM
from shlex import split as shell_split

class OSProcessThread(Thread):
    def __init__(self, service_thread, logfile):
        super().__init__()
        self._service_thread = service_thread
        self._logfile = logfile
        self._service = service_thread.service()
        self._response_queue = Queue()
        self._process = None
        self._notify = True

    def run(self):
        self._process = self._spawn_process()

        logging.info("{} >> Spawned [{}] with restart policy [{}]".format(
            self,
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

            logging.info("{} >> PID [{}] has exited with [{}]".format(
                self,
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

        self._response_queue.put((OSProcessThreadMessage.STARTED, self._process.pid))

    def _wait_until_stopped(self):
        while not self._has_stopped():
            sleep(1)

        self._response_queue.put((OSProcessThreadMessage.STOPPED, self._process.pid, self._process.returncode))

        if self._notify == True and self._service.is_to_be_restart_with(self._process.returncode):
            self._service_thread.put_request((ServiceThreadMessage.RESTART,))

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


# # # OS PROCESS COMMAD THREAD # # #

from subprocess import Popen

class OSProcessCommandThread(OSProcessThread):
    def _spawn_process(self):
        _cmd = "sh -c \"{}\"".format(self._service.command().replace('"', '\\"'))
        _args = shell_split(_cmd)
        return Popen(_args, shell=False, preexec_fn=setsid, stdout=self._logfile, stderr=self._logfile)


# # # OS PROCESS DOCKERFILE THREAD # # #

from subprocess import check_output

class OSProcessDockerfileThread(OSProcessThread):
    def _spawn_process(self):
        dockerfile_cmd = ["docker", "build", "-t", self._service.param('image'), self._service.param('path')]
        return Popen(dockerfile_cmd, shell=False, preexec_fn=setsid, stdout=self._logfile, stderr=self._logfile)

    def _has_started(self):
        _image = self._service.param('image').split(':')
        if len(_image) == 1:
            _image.append('latest')

        cmd = "docker images | awk '{{print $1$2}}' | grep \"{0}{1}\"".format(_image[0], _image[1])
        try:
            return len(check_output(cmd, shell=True)) > 0
        except:
            return False


# # # OS PROCESS DOCKER THREAD # # #

from os import remove as os_remove
from os.path import exists as os_path_exists

class OSProcessDockerThread(OSProcessThread):
    def _spawn_process(self):
        self.__cid_file_path = "docker_cids/{0}".format(self._service.name())
        docker_cmd = ["docker", "run", "-t", "--rm=true", "--cidfile=\"{0}\"".format(self.__cid_file_path), "--name=\"{0}\"".format(self._service.name())]

        # handle ports
        for port in self._service.params('port'):
            docker_cmd += ["--publish=\"{0}\"".format(port)]

        # handle expose
        for expose in self._service.params('expose'):
            docker_cmd += ["--expose=\"{0}\"".format(expose)]

        # handle link
        for link in self._service.params('link'):
            docker_cmd += ["--link=\"{0}\"".format(link)]

        # handle env
        for env in self._service.params('env'):
            docker_cmd += ["--env=\"{0}\"".format(env)]

        docker_cmd += [self._service.param('image')]

        # handle command
        command = self._service.param('command')
        if command is not None:
            docker_cmd += ["sh", "-c", command]

        return Popen(docker_cmd, shell=False, preexec_fn=setsid, stdout=self._logfile, stderr=self._logfile)

    def _post_exec(self):
        with open(self.__cid_file_path, 'r') as cid_file:
            cid = cid_file.read()

        Popen(["docker", "kill", cid], shell=False, stdout=self._logfile, stderr=self._logfile).wait()
        Popen(["docker", "rm", "-f", cid], shell=False, stdout=self._logfile, stderr=self._logfile).wait()
        os_remove(self.__cid_file_path)

    def _has_started(self):
        _image = self._service.param('image').split(':')
        if len(_image) == 1:
            _image.append('latest')

        cmd = "docker images | awk '{{print $1$2}}' | grep \"{0}{1}\"".format(_image[0], _image[1])
        try:
            return len(check_output(cmd, shell=True)) > 0 and os_path_exists(self.__cid_file_path)
        except:
            return False

    def _has_stopped(self):
        return not os_path_exists(self.__cid_file_path)


# # # OS PROCESS THREAD FACTORY

class OSProcessThreadFactory:
    @staticmethod
    def create(service_thread, logfile):
        if service_thread.service().provider() == Provider.DOCKER:
            return OSProcessDockerThread(service_thread, logfile)

        if service_thread.service().provider() == Provider.DOCKERFILE:
            return OSProcessDockerfileThread(service_thread, logfile)

        return OSProcessCommandThread(service_thread, logfile)


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
        self.__osprocess_thread = Thread()
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

        self.__osprocess_thread = OSProcessThreadFactory.create(self, self.__logfile)
        self.__osprocess_thread.start()

        logging.info("{} >> Response from [{}]: {}".format(
            self,
            self.__osprocess_thread,
            self.__osprocess_thread.get_response()
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

        if self.__osprocess_thread.is_alive():
            self.__osprocess_thread.terminate()

            logging.info("{} >> Response from [{}]: {}".format(
                self,
                self.__osprocess_thread,
                self.__osprocess_thread.get_response()
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

class Supervisor:
    def __init__(self, logfile_name="supervisor.log"):
        self.__services = {}
        self.__graph = nx.DiGraph()
        self.__logfile = open(logfile_name, "a")

    def add(self, service):
        self.__graph.add_node(service)

    def add_dependency(self, required_service, service):
        self.__graph.add_edge(required_service, service)

    def init(self):
        for service in self.__graph.nodes():
            self.__services[service] = ServiceThread(service, self.__logfile)
            self.__services[service].start()

        for edge in self.__graph.edges():
            _parent_service, _child_service = edge

            self.__services[_parent_service].put_request((ServiceThreadMessage.ADD_CHILD, self.__services[_child_service]), True)
            self.__services[_child_service].put_request((ServiceThreadMessage.ADD_DEPENDENCY, self.__services[_parent_service]), True)

    def start(self):
        for service in self.__initial_services():
            self.__services[service].put_request((ServiceThreadMessage.START,))

        while True:
            try:
                sleep(10)
            except KeyboardInterrupt:
                self.__shutdown()
                self.__logfile.close()

    def __initial_services(self):
        return [service for service in self.__graph.nodes() if len(self.__graph.in_edges(service)) == 0]

    def __shutdown(self):
        logging.info("{} >> Shutting down...".format(self))

        for service in self.__sorted_services()[::-1]:
            logging.info("{} >> Halting {} ...".format(self, service))
            self.__services[service].put_request((ServiceThreadMessage.HALT,), True)

        exit(0)

    def __sorted_services(self):
        return nx.topological_sort(self.__graph)

    def __str__(self):
        return "{}".format(self.__class__.__name__)


# # # MAIN # # #

import sys

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
