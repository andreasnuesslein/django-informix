from django.db.backends import BaseDatabaseIntrospection, FieldInfo

#from the JDBC Informix driver
SQ_TYPE_BYTE = 11
SQ_TYPE_CHAR = 0
SQ_TYPE_DATE = 7
SQ_TYPE_DATETIME = 10
SQ_TYPE_REAL = 4
SQ_TYPE_SMFLOAT = 4
SQ_TYPE_DECIMAL = 5
SQ_TYPE_NUMERIC = 5
SQ_TYPE_FLOAT = 3
SQ_TYPE_DOUBLE = 3
SQ_TYPE_INTEGER = 2
SQ_TYPE_MONEY = 8
SQ_TYPE_INTERVAL = 14
SQ_TYPE_SERIAL = 6
SQ_TYPE_SMALLINT = 1
SQ_TYPE_TEXT = 12
SQ_TYPE_VARCHAR = 13
SQ_TYPE_MASK = 31

class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Map type codes to Django Field types.
    data_types_reverse = {
        SQ_TYPE_BYTE:       'BinaryField',
        SQ_TYPE_CHAR:       'CharField',
        SQ_TYPE_DATE:       'DateField',
        SQ_TYPE_DATETIME:   'DateTimeField',
        SQ_TYPE_REAL:       'FloatField',
        SQ_TYPE_SMFLOAT:    'FloatField',
        SQ_TYPE_DECIMAL:    'DecimalField',
        SQ_TYPE_NUMERIC:    'DecimalField',
        SQ_TYPE_FLOAT:      'FloatField',
        SQ_TYPE_DOUBLE:     'FloatField',
        SQ_TYPE_INTEGER:    'IntegerField',
        SQ_TYPE_MONEY:      '??',
        SQ_TYPE_INTERVAL:   '??',
        SQ_TYPE_SERIAL:     'AutoField',
        SQ_TYPE_SMALLINT:   'SmallIntegerField',
        SQ_TYPE_TEXT:       'TextField',
        SQ_TYPE_VARCHAR:    'CharField',
        SQ_TYPE_MASK:       '??',
    }


    def get_table_list(self, cursor):
        return ['a_aanutz','a_abnutz','a_acnutz']
        # cursor.execute('SELECT tabname FROM systables')
        # return [x[0] for x in cursor.fetchall()]


    def get_table_description(self, cursor, table_name, identity_check=True):
        cursor.execute("SELECT c.* FROM syscolumns c, systables t WHERE c.tabid=t.tabid and tabname='%s'" % table_name)
        columns = [[c[0],c[3] % 256,None,c[4],c[4],None,0 if c[3] > 256 else 1] for  c in cursor.fetchall()]
        items = []
        for column in columns:
            if column[1] in [SQ_TYPE_NUMERIC, SQ_TYPE_DECIMAL]:
                column[4] = int(column[3]/256)
                column[5] = column[3] - column[4]*256
            items.append(FieldInfo(*column))
        return items

    def get_key_columns(self, cursor, table_name):
        relations = []
        cursor.execute("""
        SELECT col1.colname as column_name, t2.tabname, col2.colname as referenced_column
        FROM syscolumns col1, sysindexes idx1, sysconstraints const1, systables t1, syscolumns col2,
         sysindexes idx2, sysconstraints const2, sysreferences ref, systables t2
        WHERE col1.tabid=idx1.tabid
        AND col1.colno=idx1.part1
        AND idx1.idxname=const1.idxname
        AND const1.tabid=t1.tabid
        AND const1.constrtype='R'
        AND col2.tabid=idx2.tabid
        AND col2.colno=idx2.part1
        AND idx2.idxname=const2.idxname
        AND const2.constrid=ref.primary
        AND ref.constrid = const1.constrid
        AND t2.tabid=idx2.tabid
        AND t1.tabname = '%s'
        """ % table_name)
        relations.extend(cursor.fetchall())
        return relations

    def get_indexes(self, cursor, table_name):
        """ This query retrieves each index on the given table, including the
            first associated field name """
        cursor.execute("""select c1.colname, i1.idxtype,
                        (select constrtype from sysconstraints where idxname=i1.idxname) as pkey
                        FROM sysindexes i1, syscolumns c1
                        WHERE i1.tabid=c1.tabid AND i1.part1=c1.colno
                        AND i1.part2 = 0 and i1.tabid = (select tabid from systables where tabname='%s')""" % table_name)
        indexes = {}
        for row in cursor.fetchall():
            indexes[row[0]] = {
                'primary_key': True if row[2] == 'P' else False,
                'unique': True if row[1] == 'U' else False
            }
        return indexes

    def __get_col_index(self, cursor, schema, table_name, col_name):
        """Private method. Getting Index position of column by its name"""
        cursor.execute("""SELECT colno
                        FROM syscolumns
                        WHERE colname='%s'
                        AND tabid=(SELECT tabid FROM systables
                            WHERE tabname='%s')""" %(col_name, table_name))
        return(int(cursor.fetchone()[0]) - 1)

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        relations = {}
        kc_relations = self.get_key_columns(cursor, table_name)
        for rel in kc_relations:
            row0 = self.__get_col_index(cursor, None, table_name, rel[0])
            row1 = self.__get_col_index(cursor, None, rel[1], rel[2])
            row2 = rel[1]
            relations[row0] = (row1, row2)
        return relations


    #TODO: Just copied
    def get_constraints(self, cursor, table_name):
        constraints = {}
        schema = cursor.connection.get_current_schema()
        sql = "SELECT CONSTNAME, COLNAME FROM SYSCAT.COLCHECKS WHERE TABSCHEMA='%(schema)s' AND TABNAME='%(table)s'" % {'schema': schema.upper(), 'table': table_name.upper()}
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': True,
                    'index': False
                }
            constraints[constname]['columns'].append(colname.lower())

        sql = "SELECT KEYCOL.CONSTNAME, KEYCOL.COLNAME FROM SYSCAT.KEYCOLUSE KEYCOL INNER JOIN SYSCAT.TABCONST TABCONST ON KEYCOL.CONSTNAME=TABCONST.CONSTNAME WHERE TABCONST.TABSCHEMA='%(schema)s' and TABCONST.TABNAME='%(table)s' and TABCONST.TYPE='U'" % {'schema': schema.upper(), 'table': table_name.upper()}
        cursor.execute(sql)
        for constname, colname in cursor.fetchall():
            if constname not in constraints:
                constraints[constname] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': True,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[constname]['columns'].append(colname.lower())

        for pkey in cursor.connection.primary_keys(None, schema, table_name):
            if pkey['PK_NAME'] not in constraints:
                constraints[pkey['PK_NAME']] = {
                    'columns': [],
                    'primary_key': True,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            constraints[pkey['PK_NAME']]['columns'].append(pkey['COLUMN_NAME'].lower())

        for fk in cursor.connection.foreign_keys( True, schema, table_name ):
            if fk['FK_NAME'] not in constraints:
                constraints[fk['FK_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                    'check': False,
                    'index': False
                }
            constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
            if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)

        for index in cursor.connection.indexes( True, schema, table_name ):
            if index['INDEX_NAME'] not in constraints:
                constraints[index['INDEX_NAME']] = {
                    'columns': [],
                    'primary_key': False,
                    'unique': False,
                    'foreign_key': None,
                    'check': False,
                    'index': True
                }
            elif constraints[index['INDEX_NAME']]['unique'] :
                continue
            elif constraints[index['INDEX_NAME']]['primary_key']:
                continue
            constraints[index['INDEX_NAME']]['columns'].append(index['COLUMN_NAME'].lower())
        return constraints
