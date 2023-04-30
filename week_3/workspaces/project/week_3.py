from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]

    stocks = [Stock.from_list(s) for s in context.resources.s3.get_data(s3_key)]

    return stocks


@op(
    description="You can also pass the description of the op in the as a parameter to the op decorator, like this",
    ins={ "stocks": In(dagster_type = List[Stock], description="description of stocks input")},
    out={ "high":  Out(dagster_type = Aggregation, description="the date and value of the high")},
)
def process_data(context: OpExecutionContext, stocks: List[Stock]):
    highest = max(stocks, key=lambda k: k.high)
    high = Aggregation(date=highest.date, high=highest.high)
    return high


@op(
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    name = aggregation.date.isoformat()
    value = str(aggregation.high)

    context.resources.redis.put_data(name=name, value=value)


@op(
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
    required_resource_keys={"s3"},
    tags={"kind": "postgres"},
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    key_name = aggregation.date.isoformat()
    data = aggregation

    context.resources.s3.put_data(key_name=key_name, data=data)


@graph
def machine_learning_graph():
    data = process_data(get_s3_data())

    put_redis_data(data)
    put_s3_data(data)


local_resource_defs = {
    "s3": mock_s3_resource,
    'redis': ResourceDefinition.mock_resource()
}

local_config = {
    "resources": {
        "s3": {"config": None},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker_resource_defs = {
    "s3": s3_resource,
    "redis": redis_resource
}


@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f'prefix/stock_{partition_key}.csv'}}},
    }

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    resource_defs=local_resource_defs,
    config=local_config,
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    resource_defs=docker_resource_defs,
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
    config=docker_config,
)

machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local, cron_schedule="*/15 * * * *")

@schedule(job=machine_learning_job_docker, cron_schedule="0 * * * *")
def machine_learning_schedule_docker():
    for key in docker_config.get_partition_keys():
        yield RunRequest(
            run_key=key,
            run_config=docker_config.get_run_config_for_partition_key(partition_key=key)
                )


@sensor(
        job=machine_learning_job_docker,
        minimum_interval_seconds=30
)
def machine_learning_sensor_docker(context: SensorEvaluationContext):
    s3_prefix = "prefix"

    file_keys = get_s3_keys(
        bucket=S3["bucket"],
        prefix=s3_prefix,
        endpoint_url="http://localstack:4566",
        )
    if not file_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for file_key in file_keys:
        yield RunRequest(
                run_key=file_key,
                run_config={
                    "ops": {
                        "get_s3_data": {"config": {"s3_key": file_key}}
                        },
                    "resources": {
                        "s3": {"config": S3},
                        "redis": {"config": REDIS},
                        },
                    }
                )

