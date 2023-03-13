# claviz-python-api

Python API wrapper for Claviz.

## Installation

```
pip3 install claviz-python-api
```

## Example

```py
from claviz_python_api import ClavizClient, get_claviz_token

url = 'https://forms.claviz'
token = get_claviz_token(url, 'admin@claviz.com', 'Passw0rd')
claviz_client = ClavizClient(url, token)
print(claviz_client.get_current_user())
```
