import requests
import pandas as pd

class CodaAPI:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = "https://coda.io/apis/v1beta1"
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

    def _get_table_info(self, table_id):
        url = f"{self.base_url}/docs/{table_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Failed to get table info")

    def _get_table_data(self, table_id):
        url = f"{self.base_url}/tables/{table_id}/rows"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()["items"]
            df = pd.json_normalize(data)
            return df
        else:
            raise Exception("Failed to get table data")

    def _update_table_data(self, table_id, df):
        url = f"{self.base_url}/tables/{table_id}/rows"
        data = {"rows": df.to_dict(orient="records")}
        response = requests.put(url, headers=self.headers, json=data)
        if response.status_code == 200:
            return True
        else:
            raise Exception("Failed to update table data")

    def get_table(self, table_id):
        table_info = self._get_table_info(table_id)
        df = self._get_table_data(table_info["tables"][0]["id"])
        return df

    def set_table(self, table_id, df):
        table_info = self._get_table_info(table_id)
        return self._update_table_data(table_info["tables"][0]["id"], df)
