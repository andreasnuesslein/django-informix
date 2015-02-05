import re
from django.db.models.sql import compiler

from itertools import zip_longest
#
# # Pattern to scan a column data type string and split the data type from any
# # constraints or other included parts of a column definition. Based upon
# # <column_definition> from http://msdn.microsoft.com/en-us/library/ms174979.aspx
# _re_data_type_terminator = re.compile(
#     r'\s*\b(?:' +
#     r'filestream|collate|sparse|not|null|constraint|default|identity|rowguidcol' +
#     r'|primary|unique|clustered|nonclustered|with|on|foreign|references|check' +
#     ')',
#     re.IGNORECASE,
# )
#
#
# _re_order_limit_offset = re.compile(
#     r'(?:ORDER BY\s+(.+?))?\s*(?:LIMIT\s+(\d+))?\s*(?:OFFSET\s+(\d+))?$')
#
# # Pattern used in column aliasing to find sub-select placeholders
_re_col_placeholder = re.compile(r'\{_placeholder_(\d+)\}')
#
# _re_find_order_direction = re.compile(r'\s+(asc|desc)\s*$', re.IGNORECASE)
#
# def _remove_order_limit_offset(sql):
#     return _re_order_limit_offset.sub('',sql).split(None, 1)[1]
#
def _break(s, find):
    """Break a string s into the part before the substring to find,
    and the part including and after the substring."""
    i = s.find(find)
    return s[:i], s[i:]
#
# def _get_order_limit_offset(sql):
#     return _re_order_limit_offset.search(sql).groups()

class SQLCompiler(compiler.SQLCompiler):
    def __init__(self,*args,**kwargs):
        super(SQLCompiler,self).__init__(*args,**kwargs)
        # Pattern to find the quoted column name at the end of a field
        # specification
        #
        # E.g., if you're talking to MS SQL this regex would become
        #     \[([^\[]+)\]$
        #
        # This would match the underlined part of the following string:
        #   [foo_table][bar_column]
        #              ^^^^^^^^^^^^
        self._re_pat_col = re.compile(r"\.([^\.]+)$")


    def resolve_columns(self, row, fields=()):
        # If the results are sliced, the resultset will have an initial
        # "row number" column. Remove this column before the ORM sees it.
        if getattr(self, '_using_row_number', False):
            row = row[1:]
        values = []
        index_extra_select = len(self.query.extra_select)
        for value, field in zip_longest(row[index_extra_select:], fields):
            # print '\tfield=%s\tvalue=%s' % (repr(field), repr(value))
            if field:
                try:
                    value = self.connection.ops.convert_values(value, field)
                except ValueError:
                    pass
            values.append(value)
        return row[:index_extra_select] + tuple(values)

    def _fix_aggregates(self):
        for alias, aggregate in self.query.aggregate_select.items():
            if aggregate.sql_function == 'STDDEV_SAMP':
                self.query.aggregate_select[alias].sql_function = 'STDEV'
            elif aggregate.sql_function == 'STDDEV_POP':
                self.query.aggregate_select[alias].sql_function = 'STDEV'
            # translate Variance function names
            elif aggregate.sql_function == 'VAR_SAMP':
                self.query.aggregate_select[alias].sql_function = 'VARIANCE'
            elif aggregate.sql_function == 'VAR_POP':
                self.query.aggregate_select[alias].sql_function = 'VARIANCE'

    def as_sql(self, with_limits=True, with_col_aliases=False):
        if (with_limits and self.query.low_mark == self.query.high_mark) or self.query.high_mark == 0:
            return '', ()

        self._fix_aggregates()

        # Get out of the way if we're not a select query or there's no limiting involved.
        check_limits = with_limits and (self.query.low_mark or self.query.high_mark is not None)
        if not check_limits:
            # The ORDER BY clause is invalid in views, inline functions,
            # derived tables, subqueries, and common table expressions,
            # unless TOP or FOR XML is also specified.
            try:
                setattr(self.query, '_mssql_ordering_not_allowed', with_col_aliases)
                result = super(SQLCompiler, self).as_sql(with_limits, with_col_aliases)
            finally:
                # remove in case query is every reused
                delattr(self.query, '_mssql_ordering_not_allowed')
            return result

        raw_sql, fields = super(SQLCompiler, self).as_sql(False, with_col_aliases)

        ## replace {}
        #raw_sql = raw_sql.replace('{','').replace('}','')

        if self.query.high_mark:
            _select = "SELECT"
            _first = self.query.high_mark
            if self.query.low_mark:
                _select += " SKIP %s" % self.query.low_mark
                _first -= self.query.low_mark
            _select += " FIRST %s" % _first
            sql = raw_sql.replace("SELECT", _select,1)
            return sql, fields

    def _alias_columns(self, sql):
        """Return tuple of SELECT and FROM clauses, aliasing duplicate column names."""
        qn = self.connection.ops.quote_name

        outer = list()
        inner = list()
        names_seen = list()

        # replace all parens with placeholders
        paren_depth, paren_buf = 0, ['']
        parens, i = {}, 0
        for ch in sql:
            if ch == '(':
                i += 1
                paren_depth += 1
                paren_buf.append('')
            elif ch == ')':
                paren_depth -= 1
                key = '_placeholder_{0}'.format(i)
                buf = paren_buf.pop()

                # store the expanded paren string
                parens[key] = buf% parens
                #cannot use {} because IBM's DB2 uses {} as quotes
                paren_buf[paren_depth] += '(%(' + key + ')s)'
            else:
                paren_buf[paren_depth] += ch

        def _replace_sub(col):
            """Replace all placeholders with expanded values"""
            while _re_col_placeholder.search(col):
                col = col.format(**parens)
            return col

        temp_sql = ''.join(paren_buf)

        select_list, from_clause = _break(temp_sql, ' FROM ')

        for col in [x.strip() for x in select_list.split(',')]:
            match = self._re_pat_col.search(col)
            if match:
                col_name = match.group(1)
                col_key = col_name.lower()

                if col_key in names_seen:
                    alias = qn('{0}___{1}'.format(col_name, names_seen.count(col_key)))
                    outer.append(alias)
                    inner.append('{0} as {1}'.format(_replace_sub(col), alias))
                else:
                    outer.append(qn(col_name))
                    inner.append(_replace_sub(col))

                names_seen.append(col_key)
            else:
                raise Exception('Unable to find a column name when parsing SQL: {0}'.format(col))

        return ', '.join(outer), ', '.join(inner) + (from_clause % parens)
        #                                            ^^^^^^^^^^^^^^^^^^^^^
        # We can't use `format` here, because `format` uses `{}` as special
        # characters, but those happen to also be the quoting tokens for IBM's
        # DB2




class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass

class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
    # def as_sql(self, qn=None):
    #     self._fix_aggregates()
    #     return super(SQLAggregateCompiler, self).as_sql(qn=qn)

class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
    pass

class SQLDateTimeCompiler(compiler.SQLDateCompiler, SQLCompiler):
    pass
