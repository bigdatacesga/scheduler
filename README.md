CESGA Big Data Infrastructure
=======================

Custom Mesos Framework - Docker Executor Launcher
=======================

This project implements a full mesos framework that is able to receive offers from a mesos master and launch instances (by running the docker-executor project) with specified resources.

A REST API is provided.

Test instance execution with:

curl -X POST http://mesos_framework.service.int.cesga.es:5000/bigdata/mesos_framework/v1/instance -d '{"instance_dn": "/instances/jenes/mpi/1.0/1"}' -H "Content-type: application/json"


Get the queued instances with:

curl http://mesos_framework.service.int.cesga.es:5000/bigdata/mesos_framework/v1/instances

