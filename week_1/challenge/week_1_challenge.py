import csv
from datetime import datetime
from heapq import nlargest
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Int,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
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


@op(config_schema={"s3_key": str},
    out={
        "empty_stocks": Out(Nothing, is_required=False),
        "stocks": Out(dagster_type=List[Stock], is_required=False),
        },
    )
def get_s3_data_op(context):
    stock_list = list(csv_helper(context.op_config["s3_key"]))

    if stock_list.__len__() == 0:
        yield Output(None, output_name="empty_stocks")
    else:
        yield Output(stock_list, output_name="stocks")


@op(config_schema={"nlargest": int},
    ins={"stocks": In(dagster_type=List[Stock],
                      description="description of stocks input"),
         },
    out={"high":  Out(dagster_type=List[Aggregation],
                      description="the date and value of the high")
         },
    )
def process_data_op(context, stocks):
    n = context.op_config["nlargest"]

    highs = nlargest(n, stocks, key=lambda k: k.high)
    high_aggs = [Aggregation(date=high.date, high=high.high) for high in highs]
    return high_aggs


@op(
    description="This op hasn't been filled in yet",
    ins={
        "aggregation": In(
           dagster_type=List[Aggregation],
           description="This is the aggregation that is written to redis"
           )
        },
    out=Out(Nothing),
)
def put_redis_data_op(aggregation):
    print(f"Writing aggregation {aggregation} to redis cache....")


@op(
    description="This op hasn't been filled in yet",
    ins={
        "aggregation": In(
            dagster_type=List[Aggregation],
            description="This is the aggregation that is written to redis"
            )
        },
)
def put_s3_data_op(aggregation) -> None:
    print(f"Putting aggregation {aggregation} to S3....")


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    out=Out(Nothing),
    description="Notify if stock list is empty",
)
def empty_stock_notify_op(context: OpExecutionContext, empty_stocks: Any):
    context.log.info("No stocks returned")


@job(description="Description of job")
def machine_learning_dynamic_job():
    empty_stocks, stocks = get_s3_data_op()

    empty_stock_notify_op(empty_stocks)

    stock_agg = process_data_op(stocks)
    put_redis_data_op(stock_agg)
    put_s3_data_op(stock_agg)
