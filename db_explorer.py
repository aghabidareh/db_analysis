import json

from explorer import get_adapter
from writer import OutputWriter


def main():
    with open('config.json', 'r') as f:
        config = json.load(f)

    db_config = config['database']
    output_config = config['output']
    options = config.get('options', {})
    ignored_tables = set(options.get('ignored_tables', []))

    adapter = get_adapter(db_config['type'], db_config)
    adapter.connect()

    schemas = adapter.get_schemas()
    all_data = {}

    for schema in schemas:
        tables = adapter.get_tables(schema)
        all_data[schema] = {}

        for table_name, table_type in tables:
            if table_name in ignored_tables:
                continue

            try:
                row_count = adapter.get_table_row_count(schema, table_name)
                table_size = adapter.get_table_size(schema, table_name)
                columns = adapter.get_columns(schema, table_name)
                constraints = adapter.get_constraints(schema, table_name)
                indexes = adapter.get_indexes(schema, table_name)

                all_data[schema][table_name] = {
                    'type': table_type,
                    'row_count': row_count,
                    'size': table_size,
                    'columns': columns,
                    'constraints': constraints,
                    'indexes': indexes,
                }
            except Exception:
                continue

    writer = OutputWriter(output_config.get('format', 'json'))
    writer.write(all_data, output_config.get('structure_file', 'db_structure'))

    adapter.close()


if __name__ == "__main__":
    main()
