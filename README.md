# Taskflow

[![CircleCI](https://circleci.com/gh/mitallast/taskflow.svg?style=svg)](https://circleci.com/gh/mitallast/taskflow)

Taskflow is a service to declarative define, schedule and monitor workflows, presented as DAGs.

## Get started with Docker

Last version of service already contains [on docker hub](https://hub.docker.com/r/mitallast/taskflow/)

Just use docker-compose.yml:
```sh
docker-compose up
```

Open in browser http://localhost:8080

## Database

Taskflow requires database for persist DAG's, as default it's a postgresql server.
You can add your jdbc driver and override settings.

## Configuration

See [reference.conf](https://github.com/mitallast/taskflow/blob/master/src/main/resources/reference.conf) for it.
This is a [typesafehub/config Hocon format](https://github.com/typesafehub/config)

## Main entities

### Operations

`Operation` is a abstract parametrised unit of work - as example, abstract bash command.

Each operation defines reference configuration.

Operation can be executed with specified config and environment variables.

### Tasks

`Task` is parametrised operation - as example, bash command with specified config and environment.

Each task can define it's dependencies, retry policy, monitoring etc.

### DAG

`DAG` - Directed acyclic graph - is a task workflow definition.
Each graph contains set of `task`s, with defined dependencies between tasks.

Each dag is immutable and versioned - you cannot edit it, instead you create copy with new version -
it's required for stable, repeatable runs. Tasks in DAG also immutable and versioned. As bonus, you have
history of all DAG updates and you can simply determine wich version of DAG/task was used in concrete run of one.

As example, you have set of tasks [A, B, C, D, E].
You can create DAG like this:

```
                    +--------+
                    | task A |
                    +--------+
                     |      |
                     V      V
                +--------+ +--------+
                | task B | | task C |
                +--------+ +--------+
                     |      |
                     V      |
                +--------+  |
                | task D |  |
                +--------+  |
                     |      |
                     V      V
                   +----------+
                   | task D   |
                   +----------+

```

### DAG run

DAG execution represented as `DAG run`. like the first, is also DAG of `Task tun`
DAG run and task run has lifecycle:
 - PENDING
 - RUNNING
 - SUCCESS
 - FAILED
 - CANCELED

Taskflow guarantee at least one strategy for successful task run, or mark run as failed.
If concrete task run failed, scheduler checks it's retry strategy and retry it.
If retry strategy decides to stop retry task, it marks as failed, dag run stop pending and running tasks,
dag run will be marked as failed.

You can cancel dag run. If there's a running tasks, it's execution will be interrupted, pending tasks will be canceled.