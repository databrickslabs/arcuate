# add custom lexer for arcuate-magic
from pygments.lexers.sql import SqlLexer
from pygments.token import Name, Keyword

class ArcuateLexer(SqlLexer):
    name = 'Arcuate Magic'

    EXTRA_KEYWORDS = ['EXPERIMENT', 'MODEL', 'PANDAS', 'SPARK', 'OVERWRITE']

    def get_tokens_unprocessed(self, text):
        for index, token, value in SqlLexer.get_tokens_unprocessed(self, text):
            if token is Name and value in self.EXTRA_KEYWORDS:
                yield index, Keyword, value
            else:
                yield index, token, value

def setup(app):
    app.add_lexer('arcuate_magic', ArcuateLexer)