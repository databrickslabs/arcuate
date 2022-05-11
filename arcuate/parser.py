import sqlparse
from sqlparse.tokens import Whitespace
from typing import List
import re


def arcuate_parse(in_query: str) -> List[str]:
    """Parse an arcuate SQL string into list of tokens
    Only support limited syntax, namely CREATE SHARE/MODEL name AS/WITH
    """
    query = (
        in_query.upper()
        .replace(" EXPERIMENT ", " MODE ")
        .replace(" PANDAS ", " SELECT ")
        .replace(" SPARK ", " SELECT ")
    )

    tokens = [item.value for item in sqlparse.parse(query)[0] if item.ttype != Whitespace]

    if len(tokens) < 5:
        raise NotImplementedError("syntax not supported")

    if (
        tokens[0] not in ["CREATE", "CREATE OR REPLACE"]
        or tokens[1] not in ["SHARE", "MODE", "MODEL"]
        or tokens[3] not in ["AS", "WITH"]
    ):
        raise NotImplementedError("syntax not supported")

    pattern = re.compile(" experiment ", re.IGNORECASE)
    query = pattern.sub(" ", in_query)
    pattern = re.compile(" pandas ", re.IGNORECASE)
    query = pattern.sub(" select ", query)
    pattern = re.compile(" spark ", re.IGNORECASE)
    query = pattern.sub(" select ", query)

    tokens = sqlparse.parse(query)[0].tokens
    ids = [
        item.value.replace("'", "")
        for item in tokens
        if (str(item.ttype) == "Token.Literal.String.Single" or str(item.ttype) == "None")
        and item.value.upper() != "AS"
    ]

    return ids
