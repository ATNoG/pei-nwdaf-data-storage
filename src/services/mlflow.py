import mlflow
from src.configs.mlflow_conf import MLflowConf
from src.services.db_service import DBService

class MLflowService(DBService):
    def __init__(self) -> None:
        self.conf = MLflowConf()
        self.experiment_name = self.conf.experiment_name

    def connect(self):
        mlflow.set_tracking_uri(self.conf.tracking_uri)
        mlflow.set_experiment(self.experiment_name)

    def get_data(self, batch_number: int = 1, batch_size: int = 50) -> list:
        return []

    def write_data(self, data: dict) -> None:
        with mlflow.start_run():
            mlflow.log_metrics(data)

    def write_batch(self, data_list: list[dict]) -> None:
        with mlflow.start_run():
            for data in data_list:
                mlflow.log_metrics(data)