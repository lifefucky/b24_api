Bitrix API Client
================

A Python client for interacting with the Bitrix API.

Installation
------------

To install the client, run the following command:
```
pip install -r requirements.txt
```

Usage
-----

### Initialize the client

```python
from bitrix_client import Bitrix

hostname = ''
token_for_list = ''
bx = Bitrix(hostname=hostname, token_for_list=token_for_list)
```
### Fetch deal stages
```python
deal_ids = [2, 1000, 10000]
fields = ['ID', 'STAGE_ID']
df = bx.fetch_stages_df(deal_ids, fields)
```