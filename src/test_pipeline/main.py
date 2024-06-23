from pathlib import Path
from typing import Any
from pandas import Series, DataFrame
from prefect import task, Task, flow, get_run_logger
from dataclasses import dataclass

@dataclass
class Data:
    id: int
    x: Series
    x_df: DataFrame
    def to_dict(self) -> dict[str, Any]:
        def convert_series_to_list(obj: Any) -> Any:
            if isinstance(obj, Series):
                logger = get_run_logger()
                logger.info("converting series")
                return obj.tolist()
            elif isinstance(obj, DataFrame):
                return {col: convert_series_to_list(obj[col]) for col in obj.columns}
            return obj

        return {k: convert_series_to_list(v) for k, v in self.__dict__.items()}

class Port:
    def __init__(self, cal_fn: Task) -> None:
        raw_data = [[1.0,4,5],[1.0,2], [1.0,2], [5,6], [10, 5.0], [10.0, 100.0]]
        self.data = [Data(n, Series(x), DataFrame(x)) for n, x in enumerate(raw_data)]
        self.cal_fn = cal_fn

    def process_data(self):
        return self.cal_fn.map(self.data)

def cache_data_x(context, parameters):
    logger = get_run_logger()
    logger.info(parameters["data"].to_dict())
    return str(parameters["data"].to_dict())

@task(cache_key_fn=cache_data_x)
def cal(data: Data):
    logger = get_run_logger()
    logger.info(f"Calculating {data.to_dict()}")
    if data.x.mean() * 2 > 10:
        return data


@flow
def process_port():
    logger = get_run_logger()
    logger.info(f"{Path.cwd()}")
    port = Port(cal)
    return port.process_data()



