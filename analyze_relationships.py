import json

from analyzer import RelationshipAnalyzer
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

    analyzer = RelationshipAnalyzer(adapter, ignored_tables)
    schemas = adapter.get_schemas()

    all_relationships = {}
    all_inferred = {}
    all_stats = {}

    for schema in schemas:
        foreign_keys = analyzer.get_foreign_keys(schema)
        all_relationships[schema] = foreign_keys

        inferred = analyzer.infer_relationships(schema)
        all_inferred[schema] = inferred

        stats = analyzer.get_table_stats(schema)
        all_stats[schema] = stats

    writer = OutputWriter(output_config.get('format', 'json'))
    writer.write(all_relationships, output_config.get('relationships_file', 'db_relationships'))
    writer.write(all_inferred, f"{output_config.get('relationships_file', 'db_relationships')}_inferred")
    writer.write(all_stats, output_config.get('stats_file', 'db_stats'))

    adapter.close()


if __name__ == "__main__":
    main()
