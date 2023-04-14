from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"}
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    stocks = []
    for record in context.resources.s3.get_data(s3_key):
        stocks.append(Stock.from_list(record))

    return stocks


@op(
    description="You can also pass the description of the op in the as a parameter to the op decorator, like this",
    ins={ "stocks": In(dagster_type = List[Stock], description="description of stocks input")},
    out={ "high":  Out(dagster_type = Aggregation, description="the date and value of the high")},
)
def process_data(stocks):
    highest = max(stocks, key=lambda k: k.high)
    high = Aggregation(date=highest.date, high=highest.high)
    return high


@op(
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
    required_resource_keys={"redis"},
)
def put_redis_data(context: OpExecutionContext, aggregation: Aggregation):
    name = aggregation.date.isoformat()
    value = str(aggregation.high)

    context.resources.redis.put_data(name=name, value=value)


@op(
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
    required_resource_keys={"s3"},
)
def put_s3_data(context: OpExecutionContext, aggregation: Aggregation):
    key_name = aggregation.date.isoformat()
    data = str(aggregation.high)

    context.resources.s3.put_data(key_name=key_name, data=data)


@graph
def machine_learning_graph():
    data = process_data(get_s3_data())

    put_redis_data(data)
    put_s3_data(data)


local = {
    "resources": {
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": redis_resource
        }
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
        }
)
