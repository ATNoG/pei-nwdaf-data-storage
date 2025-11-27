"""File implements the interface for a database service"""
class DBService:

    def connect(self):
        """
        Connect to the db
        """
        raise NotImplementedError

    def get_data(self, batch_number: int = 1, batch_size: int = 50) -> list:
        """
        Get batch from db
        """
        raise NotImplementedError

    def write_data(self, data: dict) -> None:
        """
        Write a single entry to the db
        """
        raise NotImplementedError

    def write_batch(self, data_list: list[dict]) -> None:
        """
        Write a list of entries to the db
        """
        raise NotImplementedError
