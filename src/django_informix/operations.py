import datetime
from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django_informix.compiler"
    def quote_name(self, name):
        return name

    def last_insert_id(self, cursor, table_name, pk_name):
        operation = "SELECT DBINFO('sqlca.sqlerrd1') FROM SYSTABLES WHERE TABID=1"
        cursor.execute(operation)
        row = cursor.fetchone()
        last_identity_val = None
        if row is not None:
            last_identity_val = int(row[0])
        return last_identity_val

    def fulltext_search_sql(self, field_name):
        return "LIKE '%%%s%%'" % field_name

    def lookup_cast(self, lookup_type):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "LOWER(%s)"
        return "%s"

    def last_executed_query(self, cursor, sql, params):
        """
        Returns a string of the query last executed by the given cursor, with
        placeholders replaced with actual values.

        `sql` is the raw query containing placeholders, and `params` is the
        sequence of parameters. These are used by default, but this method
        exists for database backends to provide a better implementation
        according to their own quoting schemes.
        """
        return super(DatabaseOperations, self).last_executed_query(cursor, cursor.last_sql, cursor.last_params)

    def date_extract_sql(self, lookup_type, field_name):
        sqlmap = {
            'week_day': 'WEEKDAY',
            #'year': 'YEAR', # Geht leider nicht. scheint vorher abgegriffen zu werden und dann: RuntimeError: No matching overloads found. at src/native/common/jp_method.cpp:121>
            'month': 'MONTH',
            'day': 'DAY'
        }
        return "%s(%s)" % (sqlmap[lookup_type],field_name)

    def year_lookup_bounds_for_date_field(self, value):
        first = datetime.date(value, 1, 1)
        last = datetime.date(value, 12, 31)
        return [first, last]

    def start_transaction_sql(self):
        return "BEGIN WORK"

    def end_transaction_sql(self, success=True):
        return "COMMIT WORK"


    def savepoint_create_sql(self, sid):
        return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid

    def savepoint_commit_sql(self, sid):
        return "RELEASE SAVEPOINT %s" % sid

    def savepoint_rollback(self, sid):
        return "ROLLBACK TO SAVEPOINT %s" % sid


    def convert_values(self, value, field):
        if value is None or field is None:
            return value
        internal_type = field.get_internal_type()
        if internal_type == 'FloatField':
            return float(value)
        elif (internal_type and (internal_type.endswith('IntegerField')
                                 or internal_type == 'AutoField')):
            return int(value)
        if internal_type == 'DateField':
            return datetime.datetime.strptime(value,'%Y-%m-%d').date()
        if internal_type == 'DateTimeField':
            return datetime.datetime.strptime(value,'%Y-%m-%d %H:%M:%S')
        return value