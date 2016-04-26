# Maestro v1.0

requirements:

* virtualenv3
* python3

setup:

```
$ virtualenv3 -p /bin/python3 env
$ source env/bin/activate
$ pip install -r requirements.txt
```

example:

```
$ ./main.py conf.d/deploy_docker_postgresql.json
```
