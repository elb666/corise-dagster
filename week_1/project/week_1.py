import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": str},
)
def get_s3_data_op(context):
    """
    You can put the description of the op in the docstring like this
    This doesn't take an `in`, but we use config so we put `context` as a
    parameter
    """
    return list(csv_helper(context.op_config["s3_key"]))


@op(
    description="You can also pass the description of the op in the as a parameter to the op decorator, like this",
    ins={ "stocks": In(dagster_type = List[Stock], description="description of stocks input")},
    out={ "high":  Out(dagster_type = Aggregation, description="the date and value of the high")},
)
def process_data_op(stocks):
    highest = max(stocks, key=lambda k: k.high)
    high = Aggregation(date=highest.date, high=highest.high)
    return high


@op(
    description="This op hasn't been filled in yet",
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
)
def put_redis_data_op(aggregation) -> None:
    pass


@op(
    description="This op hasn't been filled in yet",
    ins={ "aggregation": In(dagster_type = Aggregation, description="This is the aggregation that is written to redis")},
)
def put_s3_data_op(aggregation) -> None:
    pass


@job(description="Description of job")
def machine_learning_job():
    data = process_data_op(get_s3_data_op())
    put_redis_data_op(data)
    put_s3_data_op(data)
