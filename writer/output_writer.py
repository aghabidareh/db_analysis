import json
import dicttoxml


class OutputWriter:
    def __init__(self, format_type='json'):
        self.format_type = format_type.lower()

    def write(self, data, filename):
        if self.format_type == 'json':
            self._write_json(data, filename + '.json')
        elif self.format_type == 'yaml':
            self._write_yaml(data, filename + '.yaml')
        elif self.format_type == 'xml':
            self._write_xml(data, filename + '.xml')
        else:
            self._write_json(data, filename + '.json')

    def _write_json(self, data, filename):
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def _write_yaml(self, data, filename):
        import yaml
        with open(filename, 'w') as f:
            yaml.dump(json.loads(json.dumps(data, default=str)), f, default_flow_style=False)

    def _write_xml(self, data, filename):
        xml_data = dicttoxml.dicttoxml(data, custom_root='database', attr_type=False)
        with open(filename, 'wb') as f:
            f.write(xml_data)
