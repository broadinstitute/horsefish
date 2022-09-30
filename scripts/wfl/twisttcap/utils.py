import google.auth as googleauth


def get_access_token():
    """Get access token."""
    creds, _ = googleauth.default()
    auth_req = googleauth.transport.requests.Request()
    creds.refresh(auth_req)

    return creds.token


def get_headers(request_type='get'):
    headers = {"Authorization": "Bearer " + get_access_token(),
                "accept": "application/json"}
    if request_type == 'post':
        headers["Content-Type"] = "application/json"
    return headers
