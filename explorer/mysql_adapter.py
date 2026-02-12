import mysql.connector

from explorer.database_adapter import DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )

    def get_schemas(self):
        if 'schema' in self.config and self.config['schema']:
            return [self.config['schema']]
        return [self.config['database']]

    def get_tables(self, schema):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            ORDER BY TABLE_NAME;
        """)
        return [(name, ttype) for name, ttype in cursor.fetchall()]

    def get_columns(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                ORDER BY ORDINAL_POSITION;
            """)
            return cursor.fetchall()
        except Exception:
            return []

    def get_constraints(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT
                    tc.CONSTRAINT_NAME,
                    tc.CONSTRAINT_TYPE,
                    kcu.COLUMN_NAME,
                    kcu.REFERENCED_TABLE_SCHEMA,
                    kcu.REFERENCED_TABLE_NAME,
                    kcu.REFERENCED_COLUMN_NAME
                FROM information_schema.TABLE_CONSTRAINTS AS tc
                LEFT JOIN information_schema.KEY_COLUMN_USAGE AS kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.TABLE_SCHEMA = '{schema}' AND tc.TABLE_NAME = '{table}'
                ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME;
            """)
            return cursor.fetchall()
        except Exception:
            return []

    def get_indexes(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT DISTINCT
                    INDEX_NAME,
                    CONCAT('INDEX ', INDEX_NAME, ' ON ', TABLE_NAME, ' (', GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), ')')
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
                GROUP BY INDEX_NAME, TABLE_NAME
                ORDER BY INDEX_NAME;
            """)
            return cursor.fetchall()
        except Exception:
            return []

    def get_table_row_count(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT TABLE_ROWS
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';
            """)
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception:
            return None

    def get_table_size(self, schema, table):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT CONCAT(ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2), ' MB')
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';
            """)
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception:
            return None

    def close(self):
        if self.conn:
            self.conn.close()
