from django.db.models.sql import compiler

class SQLCompiler(compiler.SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        if with_limits and self.query.low_mark == self.query.high_mark:
            return '', ()

        raw_sql, fields = super(SQLCompiler, self).as_sql(False, with_col_aliases, subquery)

        if with_limits:
            if self.query.high_mark is not None:
                _select = "SELECT"
                _first = self.query.high_mark

                if self.query.low_mark:
                    _select += " SKIP %s" % self.query.low_mark
                    _first -= self.query.low_mark
                _select += " FIRST %s" % _first
                raw_sql = raw_sql.replace("SELECT", _select,1)

        return raw_sql, fields


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass