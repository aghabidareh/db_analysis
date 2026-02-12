from . import PostgreSQLAdapter, MySQLAdapter, SQLServerAdapter


def get_adapter(db_type, config):
    adapters = {
        'postgresql': PostgreSQLAdapter,
        'postgres': PostgreSQLAdapter,
        'mysql': MySQLAdapter,
        'sqlserver': SQLServerAdapter,
        'mssql': SQLServerAdapter
    }
    adapter_class = adapters.get(db_type.lower())
    if not adapter_class:
        raise ValueError(f"Unsupported database type: {db_type}")
    return adapter_class(config)
