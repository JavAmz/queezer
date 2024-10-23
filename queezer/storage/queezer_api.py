import requests
from .adapter_interface import Adapter
from ..utils import eprint


class QueezerApiAdapter(Adapter):
    def __init__(self, api_key:str, validate:bool=True):
        self.api_key = api_key
        if validate:
            validation_url = "https://app.queezer.co/api/validate"
            response = requests.get(validation_url, headers={'X-QUEEZE-APIKEY': f'{self.api_key}'})
            if response.status_code == 200 and response.json().get('success'):
                eprint("Connected to Queezer")
            else:
                raise ValueError("Invalid API key provided. Authentication failed.")
        else:
            eprint("Skipping queezer connection validation!")
    
    def store(self, response, tags, args, kwargs, timestamp, duration):
        try:
            api_endpoint = "https://app.queezer.co/api/save"
            payload = {
                'request': {
                    "args": args,
                    "kwargs": kwargs
                },
                'result': response,
                'tags': tags,
                'timestamp': timestamp.microsecond//1000,
                'runtime': duration.microseconds//1000,
            }
            response = requests.post(api_endpoint, json=payload, headers={'X-QUEEZE-APIKEY': f'{self.api_key}'})
            if response.status_code == 200:
                eprint("Function call details stored successfully in the managed service.")
            else:
                eprint(response.text)
                eprint(f"Failed to store function call details in the managed service. Status code: {response.status_code}")
        except requests.RequestException as e:
            eprint(f"Error sending function call details to the managed service: {e}", flush=True)