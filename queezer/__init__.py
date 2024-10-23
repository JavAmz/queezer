import inspect
from datetime import datetime
from typing import Optional, Callable, Any, TypeVar, Generator
import os
from .storage.sqlite import SQLiteApiAdapter
from .storage.queezer_api import QueezerApiAdapter
from .storage.adapter_interface import Adapter

T = TypeVar("T")

class Queezer():
    def __init__(
            self, 
            api_key:Optional[str]=os.getenv("QUEEZER_API_KEY"), 
            adapter:Optional[Adapter]=None):
        if adapter:
            self.adapter = adapter
        elif api_key:
            self.adapter = QueezerApiAdapter(api_key)
        else:
            self.adapter = SQLiteApiAdapter(local_db_file=os.getenv("QUEEZER_LOCAL_DB", "queezer_local.db"))
            

    def squeeze(self, func:Callable[..., T], tags=[], *args, **kwargs) -> Any:
        module = inspect.getmodule(func)
        if module:
            function_name = module.__name__ + '.' + func.__qualname__
        else:
            function_name = func.__qualname__
        
        start = datetime.now()
        result = func(*args, **kwargs)
        if kwargs.get("stream", False):
            return self._squeeze_stream(result, [function_name]+tags, args, kwargs, start)
        else:
            return self._squeeze(result, [function_name]+tags, args, kwargs, start)
        
        
    def _squeeze(self, result:Any, tags:list[str], args:tuple, kwargs:dict, start:datetime) -> Any:
        response = result.to_dict()
        end = datetime.now()
        self.adapter.store(response, tags, args, kwargs, start, end-start)
        return result
        

    def _squeeze_stream(self, result:Any, tags:list[str], args:tuple, kwargs:dict, start:datetime) -> Generator[Any, None, None]:
        responses = []
        for chunk in result:
            responses.append(chunk)
            yield chunk
        end = datetime.now()
        response = {"text": "".join(r.choices[0].delta.content or "" for r in responses), "chunks": len(responses)}
        self.adapter.store(response, tags, args, kwargs, start, end-start)
