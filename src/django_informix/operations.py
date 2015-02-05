import datetime
import decimal
import time
try:
    import pytz
except:
    pytz = None

from django.conf import settings
from django.db.backends import BaseDatabaseOperations

from django.utils.encoding import smart_text
from django.utils import timezone

class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django_jdbc_informix.compiler"
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
            #'year': 'YEAR',
            'month': 'MONTH',
            'day': 'DAY'
        }
        return "%s(%s)" % (sqlmap[lookup_type],field_name)


# def __init__(self, connection):
    #     super(DatabaseOperations, self).__init__(connection)
    #     self.connection = connection
    #     self._ss_ver = None
    #     self._ss_edition = None
    #
    #

    #
    # def date_trunc_sql(self, lookup_type, field_name):
    #     return "DATEADD(%s, DATEDIFF(%s, 0, %s), 0)" % (lookup_type, lookup_type, field_name)
    #
    # def _switch_tz_offset_sql(self, field_name, tzname):
    #     """
    #     Returns the SQL that will convert field_name to UTC from tzname.
    #     """
    #
    #     if settings.USE_TZ:
    #         if pytz is None:
    #             from django.core.exceptions import ImproperlyConfigured
    #             raise ImproperlyConfigured("This query requires pytz, "
    #                                        "but it isn't installed.")
    #         tz = pytz.timezone(tzname)
    #         td = tz.utcoffset(datetime.datetime(2000, 1, 1))
    #
    #         def total_seconds(td):
    #             if hasattr(td, 'total_seconds'):
    #                 return td.total_seconds()
    #             else:
    #                 return td.days * 24 * 60 * 60 + td.seconds
    #
    #         total_minutes = total_seconds(td) // 60
    #         hours, minutes = divmod(total_minutes, 60)
    #         tzoffset = "%+03d:%02d" % (hours, minutes)
    #         field_name = "CAST(SWITCHOFFSET(TODATETIMEOFFSET(%s, '+00:00'), '%s') AS DATETIME2)" % (field_name, tzoffset)
    #     return field_name
    #
    # def datetime_trunc_sql(self, lookup_type, field_name, tzname):
    #     """
    #     Given a lookup_type of 'year', 'month', 'day', 'hour', 'minute' or
    #     'second', returns the SQL that truncates the given datetime field
    #     field_name to a datetime object with only the given specificity, and
    #     a tuple of parameters.
    #     """
    #     field_name = self._switch_tz_offset_sql(field_name, tzname)
    #     reference_date = '0' # 1900-01-01
    #     if lookup_type in ['minute', 'second']:
    #         # Prevent DATEDIFF overflow by using the first day of the year as
    #         # the reference point. Only using for minute and second to avoid any
    #         # potential performance hit for queries against very large datasets.
    #         reference_date = "CONVERT(datetime2, CONVERT(char(4), {field_name}, 112) + '0101', 112)".format(
    #             field_name=field_name,
    #         )
    #     sql = "DATEADD({lookup}, DATEDIFF({lookup}, {reference_date}, {field_name}), {reference_date})".format(
    #         lookup=lookup_type,
    #         field_name=field_name,
    #         reference_date=reference_date,
    #     )
    #     return sql, []
    #

    #

    #

    #
    #

    #
    # def max_name_length(self):
    #     return 128
    #
    # def random_function_sql(self):
    #     """
    #     Returns a SQL expression that returns a random value.
    #     """
    #     return "RAND()"
    #

    # def savepoint_create_sql(self, sid):
    #    return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid
    #
    # def savepoint_commit_sql(self, sid):
    #    return "RELEASE SAVEPOINT %s" % sid
    #
    # def savepoint_rollback(self, sid):
    #    return "ROLLBACK TO SAVEPOINT %s" % sid
    #
    # # def sql_flush(self, style, tables, sequences, allow_cascade=False):
    # #     """
    # #     Returns a list of SQL statements required to remove all data from
    # #     the given database tables (without actually removing the tables
    # #     themselves).
    # #
    # #     The `style` argument is a Style object as returned by either
    # #     color_style() or no_style() in django.core.management.color.
    # #     """
    # #     if tables:
    # #         # Cannot use TRUNCATE on tables that are referenced by a FOREIGN KEY
    # #         # So must use the much slower DELETE
    # #         from django.db import connections
    # #         cursor = connections[self.connection.alias].cursor()
    # #         # Try to minimize the risks of the braindeaded inconsistency in
    # #         # DBCC CHEKIDENT(table, RESEED, n) behavior.
    # #         seqs = []
    # #         for seq in sequences:
    # #             cursor.execute("SELECT COUNT(*) FROM %s" % self.quote_name(seq["table"]))
    # #             rowcnt = cursor.fetchone()[0]
    # #             elem = {}
    # #             if rowcnt:
    # #                 elem['start_id'] = 0
    # #             else:
    # #                 elem['start_id'] = 1
    # #             elem.update(seq)
    # #             seqs.append(elem)
    # #         cursor.execute("SELECT TABLE_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE not in ('PRIMARY KEY','UNIQUE')")
    # #         fks = cursor.fetchall()
    # #         sql_list = ['ALTER TABLE %s NOCHECK CONSTRAINT %s;' % \
    # #                 (self.quote_name(fk[0]), self.quote_name(fk[1])) for fk in fks]
    # #         sql_list.extend(['%s %s %s;' % (style.SQL_KEYWORD('DELETE'), style.SQL_KEYWORD('FROM'),
    # #                          style.SQL_FIELD(self.quote_name(table)) ) for table in tables])
    # #
    # #         if self.on_azure_sql_db:
    # #             import warnings
    # #             warnings.warn("The identity columns will never be reset " \
    # #                           "on Windows Azure SQL Database.",
    # #                           RuntimeWarning)
    # #         else:
    # #             # Then reset the counters on each table.
    # #             sql_list.extend(['%s %s (%s, %s, %s) %s %s;' % (
    # #                 style.SQL_KEYWORD('DBCC'),
    # #                 style.SQL_KEYWORD('CHECKIDENT'),
    # #                 style.SQL_FIELD(self.quote_name(seq["table"])),
    # #                 style.SQL_KEYWORD('RESEED'),
    # #                 style.SQL_FIELD('%d' % seq['start_id']),
    # #                 style.SQL_KEYWORD('WITH'),
    # #                 style.SQL_KEYWORD('NO_INFOMSGS'),
    # #                 ) for seq in seqs])
    # #
    # #         sql_list.extend(['ALTER TABLE %s CHECK CONSTRAINT %s;' % \
    # #                 (self.quote_name(fk[0]), self.quote_name(fk[1])) for fk in fks])
    # #         return sql_list
    # #     else:
    # #         return []
    #
    # #def sequence_reset_sql(self, style, model_list):
    # #    """
    # #    Returns a list of the SQL statements required to reset sequences for
    # #    the given models.
    # #
    # #    The `style` argument is a Style object as returned by either
    # #    color_style() or no_style() in django.core.management.color.
    # #    """
    # #    from django.db import models
    # #    output = []
    # #    for model in model_list:
    # #        for f in model._meta.local_fields:
    # #            if isinstance(f, models.AutoField):
    # #                output.append(...)
    # #                break # Only one AutoField is allowed per model, so don't bother continuing.
    # #        for f in model._meta.many_to_many:
    # #            output.append(...)
    # #    return output
    #
    # def start_transaction_sql(self):
    #     return "BEGIN WORK"
    #
    # def sql_for_tablespace(self, tablespace, inline=False):
    #     """
    #     Returns the SQL that will be appended to tables or rows to define
    #     a tablespace. Returns '' if the backend doesn't use t ablespaces.
    #     """
    #     return "ON %s" % tablespace
    #
    # def prep_for_like_query(self, x):
    #     """Prepares a value for use in a LIKE query."""
    #     # http://msdn2.microsoft.com/en-us/library/ms179859.aspx
    #     return smart_text(x).replace('\\', '\\\\').replace('[', '[[]').replace('%', '[%]').replace('_', '[_]')
    #
    # def prep_for_iexact_query(self, x):
    #     """
    #     Same as prep_for_like_query(), but called for "iexact" matches, which
    #     need not necessarily be implemented using "LIKE" in the backend.
    #     """
    #     return x
    #
    # def value_to_db_datetime(self, value):
    #     """
    #     Transform a datetime value to an object compatible with what is expected
    #     by the backend driver for datetime columns.
    #     """
    #     if value is None:
    #         return None
    #     if settings.USE_TZ:
    #         if timezone.is_aware(value):
    #             # pyodbc donesn't support datetimeoffset
    #             value = value.astimezone(timezone.utc)
    #     if not self.connection.features.supports_microsecond_precision:
    #         value = value.replace(microsecond=0)
    #     return value
    #
    # def value_to_db_time(self, value):
    #     if value is None:
    #         return None
    #
    #     if isinstance(value, str):
    #         return value.strip()
    #     return value.strftime('%H:%M:%S')
    #

    #
    # def value_to_db_decimal(self, value, max_digits, decimal_places):
    #     """
    #     Transform a decimal.Decimal value to an object compatible with what is
    #     expected by the backend driver for decimal (numeric) columns.
    #     """
    #     if value is None:
    #         return None
    #     if isinstance(value, decimal.Decimal):
    #         context = decimal.getcontext().copy()
    #         context.prec = max_digits
    #         #context.rounding = ROUND_FLOOR
    #         return "%.*f" % (decimal_places + 1, value.quantize(decimal.Decimal(".1") ** decimal_places, context=context))
    #     else:
    #         return "%.*f" % (decimal_places + 1, value)
    #
    # def convert_values(self, value, field):
    #     """
    #     Coerce the value returned by the database backend into a consistent
    #     type that is compatible with the field type.
    #
    #     In our case, cater for the fact that SQL Server < 2008 has no
    #     separate Date and Time data types.
    #     TODO: See how we'll handle this for SQL Server >= 2008
    #     """
    #     if value is None:
    #         return None
    #     if field and field.get_internal_type() == 'DateTimeField':
    #         return value
    #     elif field and field.get_internal_type() == 'DateField' and isinstance(value, datetime.datetime):
    #         value = value.date() # extract date
    #     elif field and field.get_internal_type() == 'TimeField' or (isinstance(value, datetime.datetime) and value.year == 1900 and value.month == value.day == 1):
    #         value = datetime.datetime.strptime(value.strip(),'%H:%M:%S').time()
    #
    #     elif isinstance(value, datetime.datetime) and value.hour == value.minute == value.second == value.microsecond == 0:
    #         value = value.date()
    #     # Force floats to the correct type
    #     elif value is not None and field and field.get_internal_type() == 'FloatField':
    #         value = float(value)
    #
    #     #print(value)
    #     return value
    #
