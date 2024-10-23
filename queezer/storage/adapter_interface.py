from abc import ABC, abstractmethod
from datetime import datetime, timedelta

class Adapter(ABC):
    
    @abstractmethod
    def store(self, response:dict, tags:list[str], args:tuple, kwargs:dict, timestamp:datetime, duration:timedelta):
        pass