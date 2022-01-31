import sqlparse
from sqlparse.tokens import Whitespace
from typing import List

def arcuate_parse(query:str) -> List[str]:
    query = query.upper().replace(' EXPERIMENT ', ' MODE ')
    tokens = [item.value for item in sqlparse.parse(query)[0] if item.ttype != Whitespace]
    if tokens[0]!= 'CREATE' or tokens[1] not in ['SHARE', 'MODE'] or tokens[3] != 'AS' or tokens[4] != 'SELECT':
        raise NotImplementedError("syntax not supported")

    id = tokens[2]
    return [id, tokens[5:]]