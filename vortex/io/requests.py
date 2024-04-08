import requests

# TODO: Refactor in Engine


def run_get_request(url, authorization):
    """
    It takes a URL and an authorization string, and returns the response from a GET request to that URL
    with the given authorization

    :param url: The URL of the API endpoint you want to call
    :param authorization: This is the base64 encoded string of the username and password
    :return: A response object
    """
    headers = {"Authorization": "Basic " + authorization}
    response = requests.get(url, headers=headers)
    return response


def run_post_request(data, url, headers):
    """
    It takes a URL, an authorization string, and a data dictionary, and returns the response from a POST request to that
    URL with the given authorization and the data in the request body

    :param url: The URL of the API endpoint you want to call
    :param authorization: This is the base64 encoded string of the username and password
    """
    response = requests.request("POST", url, headers=headers, data=data)
    return response
