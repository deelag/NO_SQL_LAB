import os
from urllib.parse import urlparse

def parse_endpoint(endpoint):
    url = urlparse(endpoint).netloc
    identifier = os.path.basename(endpoint).split(".")[0]
    return url, identifier
