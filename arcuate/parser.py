import sqlparse
from sqlparse.tokens import Whitespace
from typing import List
import re


def arcuate_parse(in_query: str) -> List[str]:
    """Parse an arcuate SQL string into list of tokens
    Only support limited syntax, namely CREATE SHARE/MODEL name AS/WITH
    """

    # replace custom arcuate keywords with sql keywords so sqlparse would accept them
    keyword_replacements = [
        (" MODEL ", " MODE "),
        (" EXPERIMENT ", " EXPLAIN "),
        (" PANDAS ", " SELECT "),
        (" SPARK ", " SELECT "),
    ]

    allowed_keywords = ["SHARE", "MODE", "EXPLAIN"]

    query = in_query

    for replacement in keyword_replacements:
        query = re.sub(replacement[0], replacement[1], query, flags=re.IGNORECASE)

    parsed = [item for item in sqlparse.parse(query)[0] if item.ttype != Whitespace]

    if len(parsed) < 5:
        raise NotImplementedError("syntax not supported")

    if (
        parsed[0].value.upper() not in ["CREATE", "CREATE OR REPLACE"]
        or parsed[1].value.upper() not in allowed_keywords
        or parsed[3].value.upper() not in ["AS", "WITH"]
    ):
        raise NotImplementedError("syntax not supported")

    ids = [
        item.value.replace("'", "")
        for item in parsed
        if str(item.ttype)
        in ["Token.Literal.String.Single", "Token.Keyword.DDL", "Token.Name.Builtin", "None"]
        and item.value.upper() != "AS"
        or item.value.upper() in allowed_keywords
    ]

    return ids
