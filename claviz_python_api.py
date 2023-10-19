import requests
import time

from typing import List, Dict, Any, Optional

def get_claviz_token(url: str, username: str, password: str) -> str:
    response = requests.get(f"{url}/api/version")
    claviz_id_app_name = response.json()["clavizIdAppName"]
    claviz_id_authority = response.json()["clavizIdAuthority"]
    token_response = requests.post(
        f"{claviz_id_authority}/api/token",
        json={
            "client_id": f"{claviz_id_app_name}.resource",
            "grant_type": "password",
            "username": username,
            "password": password,
        },
    )
    return f"Bearer {token_response.json()['access_token']}"


class ClavizClient:
    def __init__(self, url: str, token: str):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": token})
        self.url = url

    def query(self, expression: str, user_agnostic: bool = False):
        response = self.session.post(
            f"{self.url}/api/factValidation/query",
            json={"expression": expression, "userAgnostic": user_agnostic},
        )
        return response.json()

    def delete_facts(self, fact_ids: List[str]):
        self.session.post(
            f"{self.url}/api/facts/delete-multiple",
            json={"factIds": fact_ids},
        )

    def get_current_user(self) -> any:
        response = self.session.get(f"{self.url}/api/system/current-user")
        return response.json()

    def get_fact_data(self, fact_id: str) -> any:
        response = self.session.get(f"{self.url}/api/facts/fact-data?factId={fact_id}")
        return response.json()

    def get_fact_history(self, fact_id: str, page_index: int = 0, page_size: int = 10) -> any:
        response = self.session.get(
            f"{self.url}/api/facts/fact-history?factId={fact_id}&pageIndex={page_index}&pageSize={page_size}"
        )
        return response.json()

    def get_function_entities(self) -> List[any]:
        response = self.session.get(f"{self.url}/api/functionsManager/full")
        return response.json()

    def get_component_entities(self) -> List[any]:
        response = self.session.get(f"{self.url}/api/components/full")
        return response.json()

    def get_collection_entities(self) -> List[any]:
        response = self.session.get(f"{self.url}/api/collections/full")
        return response.json()

    def get_started_background_functions(self) -> List[any]:
        response = self.session.get(f"{self.url}/api/functionsManager/background")
        return response.json()

    def get_user_list(self) -> List[any]:
        response = self.session.get(f"{self.url}/api/system/user-list")
        return response.json()


    def import_facts(self, collection_id: str, facts: List[Dict[str, Any]]) -> None:
        response =  self.session.post(f"{self.url}/api/facts/import?collectionId={collection_id}&verbose=true", json=facts)
        return response.json()

    def save_fact(self, fact_id: str, collection_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        id = fact_id
        method = "PUT" if id else "POST"
        response = self.session.request(
            method=method,
            url=f"{self.url}/api/facts",
            json={"id": id, "collectionId": collection_id, "fields": fields},
        )
        return response.json()

    def start_background_function(self,function_id: str) -> None:
        self.session.put(f"{self.url}/api/functionsManager/background/{function_id}/start")


    def get_user_list(self) -> List[Dict[str, Any]]:
        response = self.session.get(f"{self.url}/api/system/user-list")
        return response.json()

    def import_facts(self, collection_id: str, facts: List[Dict[str, Any]]) -> None:
        response = self.session.post(f"{self.url}/api/facts/import?collectionId={collection_id}&verbose=true", json=facts)
        return response.json()

    def save_fact(self, fact_id: str, collection_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        id = fact_id
        method = "PUT" if id else "POST"
        response = self.session.request(
            method=method,
            url=f"{self.url}/api/facts",
            json={"id": id, "collectionId": collection_id, "fields": fields},
        )
        return response.json()

    def start_background_function(self, function_id: str) -> None:
         self.session.put(f"{self.url}/api/functionsManager/background/{function_id}/start")

    def stop_background_function(self, function_id: str) -> None:
         self.session.put(f"{self.url}/api/functionsManager/background/{function_id}/stop")

    def execute_function(self, function_id: str, parameters: Dict[str, Any] = { }, engine_name=None):
        if engine_name:
            parameters = {
                'engineName': engine_name,
                'functionId': function_id,
                'parameters': parameters
            }
            function_id = "remoteRunner"

        response = self.session.post(f"{self.url}/api/functions/{function_id}", json=parameters)

        if response.status_code == 202:
            while True:
                function_instance_status = self.get_function_instance_status(response.json()['functionInstanceId'])
                if function_instance_status['status'] == 'success' or function_instance_status['status'] == 'error':
                    self.destroy_function_instance(response.json()['functionInstanceId'])
                if function_instance_status['status'] == 'success':
                    return function_instance_status['result']
                elif function_instance_status['status'] == 'error':
                    raise Exception(function_instance_status['error'])
                time.sleep(10)

        return response.json()

    def get_function_instance_status(self, function_instance_id):
        response = self.session.get(f"{self.url}/api/functions/functionInstances/{function_instance_id}")
        return response.json()

    def destroy_function_instance(self, function_instance_id):
        self.session.delete(f"{self.url}/api/functions/functionInstances/{function_instance_id}")