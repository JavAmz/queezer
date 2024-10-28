import inspect
from datetime import datetime, timedelta
from typing import Optional, Callable, Any, TypeVar, Generator
import os
import time
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import atexit
from .storage.sqlite import SQLiteApiAdapter
from .storage.queezer_api import QueezerApiAdapter
from .storage.adapter_interface import Adapter

T = TypeVar("T")

class Queezer():
    def __init__(
            self, 
            api_key:Optional[str]=os.getenv("QUEEZER_API_KEY"), 
            adapter:Optional[Adapter]=None,
            queue_size=1000,
            max_workers=5):
        if adapter:
            self.adapter = adapter
        elif api_key:
            self.adapter = QueezerApiAdapter(api_key)
        else:
            self.local_db_file = os.getenv("QUEEZER_LOCAL_DB", "queezer_local.db")
            self.adapter = None
        
        self.store_queue = queue.Queue(maxsize=queue_size)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        
        atexit.register(self._shutdown)
    
    
    def _process_queue(self):
        while not self.stop_event.is_set() or not self.store_queue.empty():
            try:
                item = self.store_queue.get(timeout=1)  # Timeout to periodically check stop_event
                if item is None:  # Check for the shutdown signal
                    break
                response, tags, args, kwargs, start, duration = item
                retries = 3
                while retries > 0:
                    try:
                        if isinstance(self.adapter, QueezerApiAdapter):
                            adapter = self.adapter 
                        elif isinstance(self.adapter, SQLiteApiAdapter):
                            adapter = self.adapter  
                        elif self.adapter is None and self.local_db_file:
                            adapter = SQLiteApiAdapter(local_db_file=self.local_db_file) 
                            self.adapter = adapter 
                        else:
                            raise ValueError("Invalid adapter configuration")
                        
                        adapter.store(response, tags, args, kwargs, start, duration)
                        break
                    except Exception as e:
                        print(f"Error inserting function call details: {e}")
                        retries -= 1
                        time.sleep(1)  # Wait for 1 second before retrying
                self.store_queue.task_done()
            except queue.Empty:
                continue
            

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
        self.executor.submit(self._enqueue_storage, response, tags, args, kwargs, start, end-start)
        return result
        

    def _squeeze_stream(self, result:Any, tags:list[str], args:tuple, kwargs:dict, start:datetime) -> Generator[Any, None, None]:
        responses = []
        for chunk in result:
            responses.append(chunk)
            yield chunk
        end = datetime.now()
        response = {"text": "".join(r.choices[0].delta.content or "" for r in responses), "chunks": len(responses)}
        self.executor.submit(self._enqueue_storage, response, tags, args, kwargs, start, end-start)
    
    
    def _enqueue_storage(self, response:Any, tags:list[str], args:tuple, kwargs:dict, start:datetime, duration:timedelta):
        self.store_queue.put((response, tags, args, kwargs, start, duration))


    def _shutdown(self):
        self.stop_event.set()
        self.executor.shutdown(wait=True)
        if self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
        print("Shutdown complete.")