from abc import ABC, abstractmethod

class Sink(ABC):
    @abstractmethod
    def write(self, data: dict) -> bool:
        pass

    @abstractmethod
    def write_batch(self, data_list: list[dict]) -> bool:
        pass
