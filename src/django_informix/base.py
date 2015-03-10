from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseValidation
from django.db.backends.creation import BaseDatabaseCreation

import jaydebeapi as Database
import sys
from django.db import utils

from django_informix.introspection import DatabaseIntrospection
from django_informix.operations import DatabaseOperations

DatabaseError = Database.Error
IntegrityError = Database.IntegrityError

class DatabaseFeatures(BaseDatabaseFeatures):
    # needs_datetime_string_cast = False
    can_use_chunked_reads = True

    supports_microsecond_precision = False
    supports_regex_backreferencing = False
    supports_subqueries_in_group_by = False

    supports_transactions = True
    autocommits_when_autocommit_is_off = True

    #atomic_transactions =
    # TODO: sollte schon passen . Muss aber noch gefixt werden wie die genau aussehen.
    # uses_savepoints = True
    uses_savepoints = True

    supports_timezones = False
    supports_sequence_reset = False
    supports_tablespaces = True
    can_introspect_autofield = True

    #allow_sliced_subqueries = False
    #supports_paramstyle_pyformat = False
    #uses_custom_query_class = True
    #ignores_nulls_in_unique_constraints = False

    #has_bulk_insert = False


class DatabaseWrapper(BaseDatabaseWrapper):
    # _DJANGO_VERSION = _DJANGO_VERSION
    # drv_name = None
    # datefirst = 7
    Database = Database

    vendor = 'jdbc'
    operators = {
        'exact': '= %s',
        'iexact': "= LOWER(%s)",
        'contains': "LIKE %s ESCAPE '\\'",
        'icontains': "LIKE LOWER(%s) ESCAPE '\\'",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE '\\'",
        'endswith': "LIKE %s ESCAPE '\\'",
        'istartswith': "LIKE LOWER(%s) ESCAPE '\\'",
        'iendswith': "LIKE LOWER(%s) ESCAPE '\\'",

        # TODO: remove, keep native T-SQL LIKE wildcards support
        # or use a "compatibility layer" and replace '*' with '%'
        # and '.' with '_'
        'regex': 'LIKE %s',
        'iregex': 'LIKE %s',
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        options = self.settings_dict.get('OPTIONS', None)

        if options:
            self.encoding = options.get('encoding', 'utf-8')

            # make lookup operators to be collation-sensitive if needed
            self.collation = options.get('collation', None)
            if self.collation:
                self.operators = dict(self.__class__.operators)
                ops = {}
                for op in self.operators:
                    sql = self.operators[op]
                    if sql.startswith('LIKE '):
                        ops[op] = '%s COLLATE %s' % (sql, self.collation)
                self.operators.update(ops)

        self.test_create = self.settings_dict.get('TEST_CREATE', True)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation(self)
        self.creation = BaseDatabaseCreation(self)

        # self.client = DatabaseClient(self)

        #

        self.connection = None


    def get_connection_params(self):
        settings_dict = self.settings_dict

        conn_params = {
            'jclassname': 'com.informix.jdbc.IfxDriver',
            'driver_args': [settings_dict['URL'],settings_dict['USER'],settings_dict['PASSWORD']]
        }
        if settings_dict['JARS']:
            conn_params['jars'] = settings_dict['JARS']
        return conn_params

    def get_new_connection(self, conn_params):
        return Database.connect(**conn_params)

    def init_connection_state(self):
        pass

    def _set_autocommit(self, autocommit):
        print("AC ", autocommit)
        self.connection.autocommit = autocommit

    def _start_transaction_under_autocommit(self):
        """
        Start a transaction explicitly in autocommit mode.

        Staying in autocommit mode works around a bug of sqlite3 that breaks
        savepoints when autocommit is disabled.
        """
        startsql = self.ops.start_transaction_sql()
        self.cursor().execute(startsql)

    def create_cursor(self):
        return CursorWrapper(self.connection.cursor())

    # def schema_editor(self, *args, **kwargs):
    #     return MySchemaEditor(self, *args, **kwargs)

    def read_dirty(self):
        self.cursor().execute('set isolation to dirty read;')

    def read_committed(self):
        self.cursor().execute('set isolation to committed read;')

class CursorWrapper:
    def __init__(self, cursor, encoding=""):
        self.cursor = cursor
        self.last_sql = ''
        self.last_params = ()
        self.encoding = encoding

    def format_sql(self, sql, n_params=None):
            if n_params:
                try:
                    sql = sql % tuple('?' * n_params)
                except:
                    # Todo checkout whats happening here
                    pass
            else:
                if '%s' in sql:
                    sql = sql.replace('%s', '?')
            return sql

    def execute(self, sql, params=()):
        sql = self.format_sql(sql, len(params))
        self.last_sql, self.last_params = sql, params

        try:
            # print(sql,params)
            return self.cursor.execute(sql, params)
        except IntegrityError:
            e = sys.exc_info()[1]
            raise utils.IntegrityError(*e.args)
        except DatabaseError:
            e = sys.exc_info()[1]
            raise utils.DatabaseError(*e.args)


    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
