import pyodbc

from explorer.database_adapter import DatabaseAdapter


class SQLServerAdapter(DatabaseAdapter):
    def connect(self):
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.config['host']},{self.config['port']};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['user']};"
            f"PWD={self.config['password']}"
        )
        self.conn = pyodbc.connect(connection_string)

    def get_schemas(self):
        cursor = self.conn.cursor()
        if 'schema' in self.config and self.config['schema']:
            cursor.execute("""
                           SELECT SCHEMA_NAME
                           FROM INFORMATION_SCHEMA.SCHEMATA
                           WHERE SCHEMA_NAME = ?
                           ORDER BY SCHEMA_NAME;
                           """, (self.config['schema'],))
        else:
            cursor.execute("""
                           SELECT SCHEMA_NAME
                           FROM INFORMATION_SCHEMA.SCHEMATA
                           WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema', 'guest', 'db_owner', 'db_accessadmin',
                                                     'db_securityadmin', 'db_ddladmin', 'db_backupoperator',
                                                     'db_datareader', 'db_datawriter', 'db_denydatareader',
                                                     'db_denydatawriter')
                           ORDER BY SCHEMA_NAME;
                           """)
        return [row[0] for row in cursor.fetchall()]

    def get_tables(self, schema):
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT TABLE_NAME, TABLE_TYPE
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE TABLE_SCHEMA = ?
                       ORDER BY TABLE_NAME;
                       """, (schema,))
        return [(name, ttype) for name, ttype in cursor.fetchall()]

    def get_columns(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT COLUMN_NAME,
                                  DATA_TYPE,
                                  CHARACTER_MAXIMUM_LENGTH,
                                  IS_NULLABLE,
                                  COLUMN_DEFAULT,
                                  ORDINAL_POSITION
                           FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                           ORDER BY ORDINAL_POSITION;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            return []

    def get_constraints(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT tc.CONSTRAINT_NAME,
                                  tc.CONSTRAINT_TYPE,
                                  kcu.COLUMN_NAME,
                                  ccu.TABLE_SCHEMA AS foreign_table_schema,
                                  ccu.TABLE_NAME   AS foreign_table_name,
                                  ccu.COLUMN_NAME  AS foreign_column_name
                           FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                                    LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                                              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                                                  AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                                    LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
                                              ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                                                  AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                           WHERE tc.TABLE_SCHEMA = ?
                             AND tc.TABLE_NAME = ?
                           ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            return []

    def get_indexes(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT i.name AS index_name,
                                  'INDEX ' + i.name + ' ON ' + OBJECT_SCHEMA_NAME(i.object_id) + '.' +
                                  OBJECT_NAME(i.object_id)
                           FROM sys.indexes i
                           WHERE OBJECT_SCHEMA_NAME(i.object_id) = ?
                             AND OBJECT_NAME(i.object_id) = ?
                             AND i.name IS NOT NULL
                           ORDER BY i.name;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            return []

    def get_table_row_count(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT SUM(p.rows)
                FROM sys.partitions p
                INNER JOIN sys.tables t ON p.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ? AND t.name = ?
                    AND p.index_id IN (0, 1);
            """, (schema, table))
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception:
            return None

    def get_table_size(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT
                    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.indexes i ON t.object_id = i.object_id
                INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
                INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
                WHERE s.name = ? AND t.name = ?
                GROUP BY s.name, t.name;
            """, (schema, table))
            result = cursor.fetchone()
            return f"{result[0]} MB" if result else None
        except Exception:
            return None

    def close(self):
        if self.conn:
            self.conn.close()
