from base import APIClient, requests

class DremioAPI(APIClient):
    def __init__(self, server: str, username: str, password: str):
        self.server = server
        self.username = username
        self.password = password
        self.token = self.get_user_token()

    def get_user_token(self) -> str:
        endpoint = f"{self.server}/apiv2/login"
        headers = {"Content-Type": "application/json"}
        data = {"userName": self.username, "password": self.password}
        response = self.post(endpoint, headers, data)
        token = response.json().get("token")

        if not token:
            raise ValueError("Error: Failed to retrieve token. Please check your credentials and try again.")
        return token

    def post(self, endpoint: str, headers: dict, data: dict) -> requests.Response:
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        return response

    def create_space(self, name: str, description: str = "No description has been provided") -> None:
        endpoint = f"{self.server}/api/v3/catalog"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        data = {
            "entityType": "space",
            "name": name,
            "description": description
        }
        response = self.post(endpoint, headers, data)
        if 'error' in response.json():
            raise ValueError(f"Error: Failed to create space. Response: {response.json()}")
        else:
            print(f"Space '{name}' created successfully.")

    def create_view(self, name: str, path: str, query: str) -> None:
        endpoint = f"{self.server}/api/v3/catalog"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        data = {
            "entityType": "dataset",
            "path": [path, name],
            "type": "VIRTUAL_DATASET",
            "sql": query
        }
        response = self.post(endpoint, headers, data)
        if 'error' in response.json():
            raise ValueError(f"Error: Failed to create view. Response: {response.json()}")
        else:
            print(f"View '{name}' created successfully.")
            
            
username="ahmadMu"
password="passw0rd"
dremioServer="http://localhost:9047"

dremio=DremioAPI(
    server=dremioServer, username=username, password=password
)
