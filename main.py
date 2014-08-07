#!/usr/bin/env python

import logging

# # # INIT PROCESS # # #

import subprocess

class InitProcess:
    def __init__(self):
        self.__probes = []
        self.__services = []

    def add(self, command):
        service = Service(command)
        self.__services.append(service)

    def start(self):
        for service in self.__services:
            self.__start(service)

    def respawn(self, service):
        logging.info("INIT << Pong: process with ID [{0}] has a returncode [{1}]".format(service.pid(), service.returncode()))
        if service.returncode() is not 1:
            return False

        logging.info("INIT >> Respawing process with ID [{0}]".format(service.pid()))
        self.__start(service)
        return True

    def __start(self, service):
        logging.info("INIT >> Spawning process for service [{0}]".format(service.command()))
        self.__spawn_process(service)
        self.__attach_probe(service)

    def __spawn_process(self, service):
        process = subprocess.Popen(service.command(), shell=True)
        service.set_process(process)

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
        logging.info("PROBE >> Starting for process with ID [{}]".format( self.__service.pid()))
        while(True):
            logging.info("PROBE >> Ping: Heartbeat for process with ID [{}]".format(self.__service.pid()))
            self.__service.poll()
            sleep(1)

            if self.__is_terminated():
                logging.info("PROBE >> Terminating for process with ID [{}]".format(self.__service.pid()))
                return

            if self.__is_respawn():
                logging.info("PROBE >> Terminating... Respawned with a process ID [{}]".format(self.__service.pid()))
                return

    def __is_terminated(self):
        return self.__service.returncode() == 0

    def __is_respawn(self):
        return self.__init.respawn(self.__service)

# # # SERVICE # # #

class Service:
    def __init__(self, command):
        self.__command = command
        self.__process = None

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

# # # MAIN # # #

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    init_process = InitProcess()
    init_process.add("sleep 4; exit 1")
    init_process.add("sleep 8")
    init_process.start()
