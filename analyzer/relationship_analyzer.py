from explorer import PostgreSQLAdapter, SQLServerAdapter, MySQLAdapter


class RelationshipAnalyzer:
    def __init__(self, adapter, ignored_tables):
        self.adapter = adapter
        self.ignored_tables = ignored_tables

    def get_foreign_keys(self, schema):
        cursor = self.adapter.conn.cursor()

        if isinstance(self.adapter, PostgreSQLAdapter):
            cursor.execute("""
                           SELECT tc.table_name,
                                  kcu.column_name,
                                  ccu.table_name  AS foreign_table_name,
                                  ccu.column_name AS foreign_column_name,
                                  tc.constraint_name
                           FROM information_schema.table_constraints AS tc
                                    JOIN information_schema.key_column_usage AS kcu
                                         ON tc.constraint_name = kcu.constraint_name
                                             AND tc.table_schema = kcu.table_schema
                                    JOIN information_schema.constraint_column_usage AS ccu
                                         ON ccu.constraint_name = tc.constraint_name
                                             AND ccu.table_schema = tc.table_schema
                           WHERE tc.constraint_type = 'FOREIGN KEY'
                             AND tc.table_schema = %s
                           ORDER BY tc.table_name, kcu.ordinal_position;
                           """, (schema,))
        elif isinstance(self.adapter, MySQLAdapter):
            cursor.execute(f"""
                SELECT
                    tc.TABLE_NAME,
                    kcu.COLUMN_NAME,
                    kcu.REFERENCED_TABLE_NAME,
                    kcu.REFERENCED_COLUMN_NAME,
                    tc.CONSTRAINT_NAME
                FROM information_schema.TABLE_CONSTRAINTS AS tc
                JOIN information_schema.KEY_COLUMN_USAGE AS kcu
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                    AND tc.TABLE_SCHEMA = '{schema}'
                ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION;
            """)
        elif isinstance(self.adapter, SQLServerAdapter):
            cursor.execute("""
                           SELECT OBJECT_NAME(fk.parent_object_id)                             AS table_name,
                                  COL_NAME(fkc.parent_object_id, fkc.parent_column_id)         AS column_name,
                                  OBJECT_NAME(fk.referenced_object_id)                         AS foreign_table_name,
                                  COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS foreign_column_name,
                                  fk.name                                                      AS constraint_name
                           FROM sys.foreign_keys AS fk
                                    INNER JOIN sys.foreign_key_columns AS fkc
                                               ON fk.object_id = fkc.constraint_object_id
                                    INNER JOIN sys.tables t
                                               ON fk.parent_object_id = t.object_id
                                    INNER JOIN sys.schemas s
                                               ON t.schema_id = s.schema_id
                           WHERE s.name = ?
                           ORDER BY table_name;
                           """, (schema,))

        relationships = []
        for row in cursor.fetchall():
            if row[0] not in self.ignored_tables and row[2] not in self.ignored_tables:
                relationships.append({
                    'table': row[0],
                    'column': row[1],
                    'foreign_table': row[2],
                    'foreign_column': row[3],
                    'constraint_name': row[4]
                })
        return relationships

    def get_table_stats(self, schema):
        stats = []
        tables = self.adapter.get_tables(schema)

        for table_name, _ in tables:
            if table_name in self.ignored_tables:
                continue

            try:
                row_count = self.adapter.get_table_row_count(schema, table_name)
                table_size = self.adapter.get_table_size(schema, table_name)

                stats.append({
                    'table': table_name,
                    'size': table_size,
                    'row_count': row_count
                })
            except Exception:
                continue

        stats.sort(key=lambda x: x.get('row_count', 0) or 0, reverse=True)
        return stats

    def infer_relationships(self, schema):
        cursor = self.adapter.conn.cursor()

        if isinstance(self.adapter, PostgreSQLAdapter):
            cursor.execute("""
                           SELECT c.table_name,
                                  c.column_name,
                                  c.data_type
                           FROM information_schema.columns c
                           WHERE c.table_schema = %s
                             AND c.column_name LIKE '%%_id'
                             AND c.column_name != 'id'
                           ORDER BY c.table_name, c.column_name;
                           """, (schema,))
        elif isinstance(self.adapter, MySQLAdapter):
            cursor.execute(f"""
                SELECT
                    c.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE
                FROM information_schema.COLUMNS c
                WHERE c.TABLE_SCHEMA = '{schema}'
                    AND c.COLUMN_NAME LIKE '%%_id'
                    AND c.COLUMN_NAME != 'id'
                ORDER BY c.TABLE_NAME, c.COLUMN_NAME;
            """)
        elif isinstance(self.adapter, SQLServerAdapter):
            cursor.execute("""
                           SELECT c.TABLE_NAME,
                                  c.COLUMN_NAME,
                                  c.DATA_TYPE
                           FROM INFORMATION_SCHEMA.COLUMNS c
                           WHERE c.TABLE_SCHEMA = ?
                             AND c.COLUMN_NAME LIKE '%_id'
                             AND c.COLUMN_NAME != 'id'
                           ORDER BY c.TABLE_NAME, c.COLUMN_NAME;
                           """, (schema,))

        potential_fks = cursor.fetchall()

        if isinstance(self.adapter, PostgreSQLAdapter):
            cursor.execute("""
                           SELECT table_name
                           FROM information_schema.tables
                           WHERE table_schema = %s
                             AND table_type = 'BASE TABLE';
                           """, (schema,))
        elif isinstance(self.adapter, MySQLAdapter):
            cursor.execute(f"""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = '{schema}'
                    AND TABLE_TYPE = 'BASE TABLE';
            """)
        elif isinstance(self.adapter, SQLServerAdapter):
            cursor.execute("""
                           SELECT TABLE_NAME
                           FROM INFORMATION_SCHEMA.TABLES
                           WHERE TABLE_SCHEMA = ?
                             AND TABLE_TYPE = 'BASE TABLE';
                           """, (schema,))

        all_tables = {row[0] for row in cursor.fetchall() if row[0] not in self.ignored_tables}

        inferred_relationships = []

        for table_name, column_name, data_type in potential_fks:
            if table_name in self.ignored_tables:
                continue

            potential_table = column_name[:-3]

            singular_forms = [
                potential_table,
                potential_table + 's',
                potential_table + 'es',
                potential_table[:-1] if potential_table.endswith('ies') else None,
            ]

            if '_' in potential_table:
                parts = potential_table.split('_')
                singular_forms.append(parts[-1])
                singular_forms.append(parts[-1] + 's')

            matched_table = None
            for form in singular_forms:
                if form and form in all_tables:
                    matched_table = form
                    break

            if matched_table:
                inferred_relationships.append({
                    'from_table': table_name,
                    'from_column': column_name,
                    'to_table': matched_table,
                    'to_column': 'id',
                    'confidence': 'high' if potential_table == matched_table else 'medium'
                })

        return inferred_relationships
