import pandas as pd 
import requests
import snowflake.connector
from datetime import datetime,date
import numpy as np
import os



### Provides connection details to sandbox to push/pull 
### data into LMS_QUOTE, LMS_QUOTE_ITEMS, and LMS_QUOTE_SCOPE

def get_snowflake_connection():
# Connection details
    #USER = os.environ['USER']
    #PASSWORD = os.environ['PASSWORD']
    USER = 'MASCJER'
    PASSWORD = 'LucySnickers!1216'
    ACCOUNT = "sandbox_chrobinson.east-us-2.azure"
    WAREHOUSE = "SURFACETRANS_WAREHOUSE"
    SCHEMA = "BASE"
    DATABASE = "SANDBOX_NAST_LTL_DOMAIN"
    VAULT_URL = "https://vault-prod.centralus.chrazure.cloud"
    ROLE = "CHR_OWNER"

    # Get client token
    resp = requests.post(f"{VAULT_URL}/v1/auth/okta/login/{USER.lower()}", json={"password": PASSWORD}, verify=False)
    assert resp.status_code == 200, "Failed to get client token"
    client_token = resp.json()["auth"]["client_token"]
    # Get leased password
    resp = requests.get(
        f"{VAULT_URL}/v1/database/static-creds/snowflake_sandbox_{USER.lower()}",
        headers={"X-Vault-Token": client_token},
        verify=False
    )

    assert resp.status_code == 200, "Failed to get leased password"
    leased_password = resp.json()["data"]["password"]



    connection = snowflake.connector.connect(
            user=f"{USER.upper()}@CHROBINSON.COM",
            password=leased_password,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            schema=SCHEMA,
            database=DATABASE,
            role=ROLE

        )
    return connection