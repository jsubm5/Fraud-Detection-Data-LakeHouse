from abc import ABC, abstractmethod
import requests


class APIClient(ABC):
    @abstractmethod
    def post(self, endpoint: str, headers: dict, data: dict):
        pass