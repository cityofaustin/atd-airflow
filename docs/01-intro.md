# The DAG

#### DAG (Directed Acyclic Graph)

```
In Math, Computer science: A finite directed graph that does not have any loops.

In Airflow:
	It represents a collection of tasks to run, oranized in a way that represent their dependencies and relationships.

	Each Node is a task

	Each Ndge is a dependency
```



An example:

- Node A could be downloading data
- Node B could be sending data for processing
- Node C could be monitoring the data processing
- Node D could be generating a report
- Node E could be sending an email to the DAG's owner.

```
(A) -> (B) -> (C) -> (D) -> (E)
```

   |----------- The DAG -----------|

#### DAG's don't have loops! Therefore this is not a dag, because if E then moved on to run C, it would become an infinite loop:

```text
               |-------------|
               ▽             △
(A) -> (B) -> (C) -> (D) -> (E)
```



#### DAGs are defined in python files in your DAG_FOLDER (usually ~/airflow/dags)

DAG properties:
- dag_id - The unique identifier for your DAG
- description - the description of your DAG
- start_date - establishes when the DAG should start
- schedule interval - defines how often your DAG should run
- depend_on_past - run the next DAGRun if the previous is completed successfully.
- default_args - a dictionary of values to be used as the constuctor keyword parameter when initializing operators


### Executors

#### Sequential Executor:

The Sequential Executor runs a single task instance at a time in a linear fashion with no parallelism functionality (A → B → C). It does identify a single point of failure, making it helpful for debugging. Otherwise, the Sequential Executor is not recommended for any use cases that require more than a single task execution at a time.


#### Local Executor: 

The easy option, completes tasks in parallel that run on a single machine, the same machine that houses the Scheduler and all code necessary to execute. A single LocalWorker picks up and runs jobs as they’re scheduled and is fully responsible for all task execution.

In practice, this means that you don't need resources outside of that machine to run a DAG or a set of DAGs (even heavy workloads).

- Pros:
    - It still offers parallelism 
    - It's straightforward and easy to set up
    - It's cheap and resource light
- Cons:
    - It's not (as) scalable
    - It's dependent on a single point of failure

While the LocalExecutor is a great way to save on engineering resources for testing even with a heavy workload, we generally recommend going with Celery for running DAGs in production, especially if you're running anything that's time sensitive.

#### Celery Executor:

At its core, Airflow's CeleryExecutor is built for horizontal scaling.

Celery itself is a way of running python processes in a distributed fashion. To optimize for flexibility and availability, the CeleryExecutor works with a "pool" of independent workers across which it can delegate tasks, via messages. On Celery, your deployment's scheduler adds a message to the queue and the Celery broker delivers it to a Celery worker (perhaps one of many) to execute.

If a worker node is ever down or goes offline, the CeleryExecutor quickly adapts and is able to assign that allocated task or tasks to another worker.

If you're running native Airflow, adopting a CeleryExecutor means you'll have to set up an underlying database to support it (RabbitMQ/Redis). If you're running on Astronomer, the switch really just means your deployment will be a bit heavier on resources (and price) - and that you'll likely have to keep a closer eye on your workers.

- Pros:
    - High availability
    - Built for horizontal scaling
- Cons:
    - Harder to maintain
    - It's pricier
    - It takes some work to set up
    - Worker maintenance

### Operators

An operator is basically a single task. Think of a DAG as a collection of tasks, therefore a DAG is a collection of operators.

Operators are defined in python, but they can be written in many different languages and tools.

- Key Points:
    - An operator defines a single task
    - It should be idempotent (meaning your operator should produce the same result regardless of how many times it is run, or where it is run)
    - It should be ready to retry automatically (in case of failure)
    - It is instantiated using the Operator class.
    - When instantiated, this task becomes part of the DAG.


Airflow provides many operators:
- BashOperator (executes a bash command)
- PythonOperator (Calls an arbitrary Python function)
- EmailOperator (Sends an email)
- MySqlOperator, SqliteOperator, PostgreOperator (to execute SQL commands)
- DockerOperator (connects to a docker server and runs a container)
- Etc.

Types of Operators
- *Action*: 
    - Action operators perform an action (ie. BashOperator, PythonOperator, EmailOperator,...)
- *Transfer Operators*:
    - Transfer operators move data from one system to another (ie. PrestoToMysqlOperator, SftpOperator,...)
    - don't use Transfer Operators for large amounts of data, this is because the data is loaded into memory and you can overwhelm the machine.
- *Sensor Operators*: 
    - These wait for data to arrive at a defined location. These are used to check on the state of the process.
    - They are useful for monitoring external processes, like waiting for files to be uploaded.
    - They are basically long-running tasks.
    
All Operators inherit from the BaseOperator class. 




