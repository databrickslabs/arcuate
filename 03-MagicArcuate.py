# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Whitespace

# COMMAND ----------

from typing import List

def arcuate_parse(query:str) -> List[str]:
    query = query.replace(' MODEL ', ' MODE ')
    tokens = [item.value.upper() for item in sqlparse.parse(query)[0] if item.ttype != Whitespace]
    if tokens[0]!= 'CREATE' or tokens[1] not in ['SHARE', 'MODE'] or tokens[3] != 'AS' or tokens[4] != 'SELECT':
        raise NotImplementedError("syntax not supported")

    id = tokens[2]
    return [share_id, tokens[5:]]

# COMMAND ----------

query = 'create share `share  id` as select model model_id'
arcuate_parse(query)

# COMMAND ----------

query = 'create MODEL model_id as select * from share share_id'
arcuate_parse(query)
