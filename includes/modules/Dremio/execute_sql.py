import requests

class Dremio_sql:
    def __init__(
        self, 
        username: str,
        password: str,
        dremio_endpoint: str
    ):
        self.username=username
        self.password=password
        self.endpoint=dremio_endpoint
        
    def _get_dremio_token(self):
        response = requests.post(
            f'{self.endpoint}/apiv2/login', 
            json={'userName': self.username, 'password': self.password})
        if response.status_code == 200:
            return response.json()['token']
        else:
            raise Exception(f"Failed to get token: {response.text}")

    def execute_sql(self, sql:str):
        token = self._get_dremio_token()
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'_dremio{token}'
        }
        response = requests.post(f'{self.endpoint}/api/v3/sql', headers=headers, json={'sql': sql})
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to execute query: {response.text}")
