import psycopg2

from explorer.database_adapter import DatabaseAdapter


class PostgreSQLAdapter(DatabaseAdapter):
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        cursor = self.conn.cursor()
        cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        cursor.execute(f"SET statement_timeout = '{self.config.get('timeout', 60)}s'")
        self.conn.commit()

    def get_schemas(self):
        cursor = self.conn.cursor()
        if 'schema' in self.config and self.config['schema']:
            cursor.execute("""
                           SELECT schema_name
                           FROM information_schema.schemata
                           WHERE schema_name = %s
                           ORDER BY schema_name;
                           """, (self.config['schema'],))
        else:
            cursor.execute("""
                           SELECT schema_name
                           FROM information_schema.schemata
                           WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                           ORDER BY schema_name;
                           """)
        return [row[0] for row in cursor.fetchall()]

    def get_tables(self, schema):
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT table_name, table_type
                       FROM information_schema.tables
                       WHERE table_schema = %s
                       ORDER BY table_name;
                       """, (schema,))
        return [(name, ttype) for name, ttype in cursor.fetchall()]

    def get_columns(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT column_name,
                                  data_type,
                                  character_maximum_length,
                                  is_nullable,
                                  column_default,
                                  ordinal_position
                           FROM information_schema.columns
                           WHERE table_schema = %s
                             AND table_name = %s
                           ORDER BY ordinal_position;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            self.conn.rollback()
            return []

    def get_constraints(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT tc.constraint_name,
                                  tc.constraint_type,
                                  kcu.column_name,
                                  ccu.table_schema AS foreign_table_schema,
                                  ccu.table_name   AS foreign_table_name,
                                  ccu.column_name  AS foreign_column_name
                           FROM information_schema.table_constraints AS tc
                                    LEFT JOIN information_schema.key_column_usage AS kcu
                                              ON tc.constraint_name = kcu.constraint_name
                                                  AND tc.table_schema = kcu.table_schema
                                    LEFT JOIN information_schema.constraint_column_usage AS ccu
                                              ON ccu.constraint_name = tc.constraint_name
                                                  AND ccu.table_schema = tc.table_schema
                           WHERE tc.table_schema = %s
                             AND tc.table_name = %s
                           ORDER BY tc.constraint_type, tc.constraint_name;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            self.conn.rollback()
            return []

    def get_indexes(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT i.indexname,
                                  i.indexdef
                           FROM pg_indexes i
                           WHERE i.schemaname = %s
                             AND i.tablename = %s
                           ORDER BY i.indexname;
                           """, (schema, table))
            return cursor.fetchall()
        except Exception:
            self.conn.rollback()
            return []

    def get_table_row_count(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                           SELECT reltuples::bigint
                           FROM pg_class
                           WHERE oid = %s::regclass
                           """, (f"{schema}.{table}",))
            return cursor.fetchone()[0]
        except Exception:
            self.conn.rollback()
            return None

    def get_table_size(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT pg_size_pretty(pg_total_relation_size(%s || '.' || %s))
            """, (schema, table))
            return cursor.fetchone()[0]
        except Exception:
            self.conn.rollback()
            return None

    def close(self):
        if self.conn:
            self.conn.close()
