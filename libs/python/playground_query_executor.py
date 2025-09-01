"""
Script to execute Queries on Amorphic Playground via Amorphic APIs.
"""

import io
import time
import json
import base64
import logging
import requests
import pandas as pd


logging.basicConfig(level="INFO")
LOGGER = logging.getLogger()


class PlaygroundQueryExecutor:
    """
    Executes queries on Playground
    """

    def __init__(
        self,
        api,
        query,
        work_group="primary",
        query_target_location="athena",
        assume_role="no",
        retry=5,
        delay=60,
        logger=None
    ):
        """

        :param api:
        :param query: SQL Query String
        :param work_group: primary or AmazonAthenaEngineV3
        :param query_target_location: 'redshift' or 'athena'
        :param assume_role: 'yes' uses User IAM role.
        :param retry: No of times to retry before checking query status again.
        :param delay: Delay in seconds for each retry.
        """
        self.api = api
        self.query = query
        self.work_group = work_group
        self.query_target_location = query_target_location
        self.assume_role = assume_role
        self.retry = retry
        self.delay = delay
        self._logger = LOGGER if not logger else logger

    def _custom_json_response(self, response,exitcode=0, message=None):
        """
        _custom_json_response : generate a custom json response for requests
        response object. 
        """
        try:
            if hasattr(response, "status_code"):
                if response.status_code == 200:
                    return {
                        "exitcode": 0,
                        "message": "Success",
                        "data": response.json(),
                        "response": response
                    }
                else:
                    return {
                        "exitcode": response.status_code,
                        "message": f"Failed with error {response.reason}",
                        "data": response.json(),
                        "response": response
                    }
            else:
                return {
                    "exitcode": exitcode,
                    "message": message,
                    "data": response
                }
        except Exception as ex:
            return {
                "exitcode": 1,
                "message": f"Failed to parse response: {ex}",
                "data": None
            }

    def _safe_api_call(self, **request_kwargs):
        """
        make a safe API call using the provided api instance.
        """
        response = self.api.make_request(**request_kwargs)
        json_resp = self._custom_json_response(response)
        if json_resp["exitcode"] != 0:
            raise Exception(f"API call failed: {json_resp['message']}")
        return json_resp

    def build_query_payload(self):
        """Build and encode the query payload."""
        self._logger.info("In PlaygroundQueryExecutor.build_query_payload")
        self._logger.debug(
            "Query: %s, WorkGroup: %s, Target: %s, AssumeRole: %s",
            self.query,
            self.work_group,
            self.query_target_location,
            self.assume_role,
        )
        # Encoding the queries from plain text to base64 string
        # to support Amorphic encoding standards

        query_bytes = self.query.encode("ascii")
        query_base64_bytes = base64.b64encode(query_bytes)
        query_base64_string = query_base64_bytes.decode("ascii")
        if self.query_target_location == "athena":
            payload = json.dumps(
                {
                    "QueryString": query_base64_string,
                    "Encoding": "base64",
                    "WorkGroup": self.work_group,
                    "AssumeRole": self.assume_role,
                }
            )
        else:
            payload = json.dumps(
                {
                    "QueryString": query_base64_string,
                    "Encoding": "base64",
                    "QueryTargetLocation": self.query_target_location,
                }
            )
        self._logger.info("In build_query_payload,Encoded Query")

        return payload

    def submit_query(self, payload):
        """
        Submit the encoded query payload.
        :param payload: base64 encoded query string.
        :return: response from `requests` library
        """
        self._logger.info("In PlaygroundQueryExecutor.submit_query, Submitting query")
        headers = {
            "Content-Type": "application/json",
        }
        api_endpoint = f"{self.api._url}/queries"
        self._logger.debug("In PlaygroundQueryExecutor.submit_query,"\
            "Method: 'POST', Endpoint: %s\n Payload : %s",api_endpoint,payload,)

        response = self._safe_api_call(
            method="POST",
            custom_url=api_endpoint,
            data=payload,
            headers=headers,
        )
        return response

    def poll_query_status(self, query_id, retry=1):
        """
        Poll the status of a submitted query.
        :param query_id: Query ID
        :param retry: No of times to retry
        :return: response from `requests` library. Returns QueryStatus.
        """
        self._logger.info(
            "In PlaygroundQueryExecutor.poll_query_status, " \
            "Polling query status: %s",
            query_id,
        )
        headers = {
            "Content-Type": "application/json",
        }
        api_endpoint = f"{self.api._url}/queries/{query_id}"
        response = self._safe_api_call(
            method="GET",
            custom_url=api_endpoint,
            headers=headers,
        )
        query_status = response["data"].get("QueryStatus")
        time.sleep(20)

        while query_status == "RUNNING" and retry <= self.retry:
            response = self._safe_api_call(
                method="GET",
                custom_url=api_endpoint,
                headers=headers,
            )
            query_status = response["data"].get("QueryStatus")
            if query_status != "RUNNING":
                return response
            self._logger.info(
                "Retry: %s, Status: %s",
                retry,
                query_status,
            )
            retry += 1
            time.sleep(self.delay)

        self._logger.warning("Max retries reached")
        return response

    def run(self):
        """
        Run the full query execution process.
        :return: response from `requests` library. Returns QueryStatus.
        """
        self._logger.info("In PlaygroundQueryExecutor.run, Running query")
        query_payload = self.build_query_payload()
        response = self.submit_query(query_payload)
        query_id = response["data"]["QueryId"]
        query_status_response = self.poll_query_status(query_id)
        if query_status_response["data"]["QueryStatus"] in ["SUCCEEDED"]:
            return query_status_response
        elif query_status_response["data"]["QueryStatus"] == "RUNNING":
            print(
                f"Retries exhausted for query {query_id}. Query is still\
                running."
            )
            return query_status_response
        else:
            self._logger.error(
                "Query failed: %s", query_status_response["data"]["QueryStatus"]
            )
            raise Exception(query_status_response["data"].get("Message"))

    def fetch_query_results(self):
        """
        Run the query and return the results as a pandas DataFrame.
        :return: pandas DataFrame containing the query results
        """
        self._logger.info(
            "In PlaygroundQueryExecutor.fetch_query_results, " \
            "Fetching results for query on endpoint: %s, target: %s",
            self.api._url,
            self.query_target_location,
        )

        query_response = self.run()
        query_status = query_response["data"]["QueryStatus"]
        if query_status == "SUCCEEDED":
            download_link = query_response["data"]["ResultsDownloadLink"]
            download_response = requests.get(download_link)
            data = io.StringIO(download_response.text)
            pd_df = pd.read_csv(data, sep=",")
            return pd_df
        elif query_status == "RUNNING":
            self._logger.error(
                "Query is still running. Increase delay or retry settings and try again."
            )
            raise Exception("Query still running after retries.")
        elif query_status == "FAILED":
            error_message = query_response["data"].get("Message", "Unknown error")
            raise Exception(error_message)
        else:
            status_message = query_response["data"].get("Message", "")
            print(f"Query Status: {query_status} | Message: {status_message}")
