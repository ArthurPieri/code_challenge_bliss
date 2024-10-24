import json

from interfaces.extract_interface import ExtractInterface
from interfaces.my_log import LoggingEtl


class ExtractJson(ExtractInterface, LoggingEtl):
    def __init__(self):
        self.log = LoggingEtl().start_logging()

    def extract(
        self,
        **kwargs,
    ) -> list[dict]:
        """
        Get data from Json file and return it as a list of dicts

        ## Kwargs
        """
        return self._get_connection(**kwargs)

    def _get_connection(self, **kwargs) -> list:
        """
        Read the file and return its content

        ## Kwargs:
        - json_file: complete path to file

        ## Exceptions:
        - AssertionError:
        """
        assert "json_file" in kwargs
        with open(kwargs["json_file"]) as file:
            data = json.load(file)
        self.log.info("File: %s, Len: %s", kwargs["json_file"], len(data))
        return data


if __name__ == "__main__":
    e = ExtractJson()
    e.extract(json_file="../payments.json")
