import datetime
import uuid
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.models import Aggregate
from django.db.backends import utils as backend_utils
from django.utils.dateparse import parse_date, parse_datetime, parse_time


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

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            return "LOWER(%s)"
        return "%s"

    def check_expression_support(self, expression):
        if isinstance(expression,Aggregate):
            if expression.function in ['STDDEV_POP','STDDEV_SAMP']:
                expression.function = 'STDDEV'
            if expression.function in ['VAR_POP','VAR_SAMP']:
                expression.function = 'VARIANCE'

    def date_extract_sql(self, lookup_type, field_name):
        sqlmap = {
            'week_day': 'WEEKDAY',
            'month': 'MONTH',
            'day': 'DAY'
        }
        return "%s(%s)" % (sqlmap[lookup_type],field_name)

    def year_lookup_bounds_for_date_field(self, value):
        # first = datetime.date(value, 1, 1)
        # last = datetime.date(value, 12, 31)

        # TODO: https://code.djangoproject.com/ticket/24596 will hopefully fix this.
        first = '%s-01-01' % value
        last = '%s-12-31' % value
        return [first, last]

    def start_transaction_sql(self):
        return "BEGIN WORK"

    def end_transaction_sql(self, success=True):
        return "COMMIT WORK"

    def savepoint_create_sql(self, sid):
        return "SAVEPOINT %s ON ROLLBACK RETAIN CURSORS" % sid

    def savepoint_commit_sql(self, sid):
        return "RELEASE SAVEPOINT %s" % sid

    def savepoint_rollback_sql(self, sid):
        return "ROLLBACK TO SAVEPOINT %s" % sid

    def get_db_converters(self, expression):
        #print(print(expression)
        converters = super(DatabaseOperations, self).get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == 'BooleanField':
            converters.append(lambda value,*_: True if value == 1 else False)
        elif internal_type == 'NullBooleanField':
            converters.append(lambda value,*_: True if value == 1 else False if value == 0 else None)
        elif internal_type == 'DateTimeField':
             converters.append(self.convert_datetimefield_value)
        elif internal_type == 'DateField':
            converters.append(self.convert_datefield_value)
        elif internal_type == 'TimeField':
            converters.append(self.convert_timefield_value)
        elif internal_type == 'DecimalField':
            converters.append(self.convert_decimalfield_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        return converters

    def convert_decimalfield_value(self, value, expression, connection, context):
        return backend_utils.typecast_decimal(expression.output_field.format_number(value))

    def convert_datefield_value(self, value, expression, connection, context):
        if value is not None and not isinstance(value, datetime.date):
            value = parse_date(value)
        return value

    def convert_datetimefield_value(self, value, expression, connection, context):
        if value is not None and not isinstance(value, datetime.datetime):
            value = parse_datetime(value)
        return value

    def convert_timefield_value(self, value, expression, connection, context):
        if value is not None and not isinstance(value, datetime.time):
            value = parse_time(value)
        return value

    def convert_uuidfield_value(self, value, expression, connection, context):
        if value is not None:
            value = uuid.UUID(value)
        return value