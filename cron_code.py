import requests
import json


def fetch_statement():
    statement_initiate_endpoint = "http://0.0.0.0:8000/statement/initiate"
    statement_retrieval_endpoint = "http://0.0.0.0:8000/statement/retrieval"

    initiate_headers = {"Content-Type": "application/json"}
    retrieval_headers = {"Content-Type": "application/json"}
    initiate_data = {
        "data": {
            "fromDate": "2022-11-11",
            "toDate": "2022-11-12"
        }
    }
    retrieval_data = {
        "data": {
            "statementId": "111111111"
        }
    }
    initiate_request = requests.post(url=statement_initiate_endpoint, headers=initiate_headers, json=initiate_data)
    initiate_response = initiate_request.json()

    retrieval_response = requests.post(url=statement_retrieval_endpoint, headers=retrieval_headers, json=retrieval_data)
    response = retrieval_response.json()
    print(response[0])
    print(type(response[0]))


fetch_statement()