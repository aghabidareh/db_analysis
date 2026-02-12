from abc import ABC, abstractmethod


class DatabaseAdapter(ABC):
    def __init__(self, config):
        self.config = config
        self.conn = None

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_schemas(self):
        pass

    @abstractmethod
    def get_tables(self, schema):
        pass

    @abstractmethod
    def get_columns(self, schema, table):
        pass

    @abstractmethod
    def get_constraints(self, schema, table):
        pass

    @abstractmethod
    def get_indexes(self, schema, table):
        pass

    @abstractmethod
    def get_table_row_count(self, schema, table):
        pass

    @abstractmethod
    def get_table_size(self, schema, table):
        pass

    @abstractmethod
    def close(self):
        pass
