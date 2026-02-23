class Conf:
    @classmethod
    def load_env(cls, file: str = ".env") -> None:
        """
        Load configuration from env
        """
        raise NotImplementedError

    @classmethod
    def get(cls) -> dict:
        """
        Get a dict with the loaded confs.
        If not loaded yet, then load in the function
        """
        raise NotImplementedError

    @classmethod
    def load(cls) -> None:
        cls.load_env()
