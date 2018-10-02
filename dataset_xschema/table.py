from dataset.table import Table

from copy import deepcopy
import os

from .util import parse_record
from .constraint import ConstraintMapping
from .exception import NonUniformTypeException, NotNullException, NotUniqueException


class XSchemaTable(Table):
    def __init__(self, *args, **kwargs):
        super(XSchemaTable, self).__init__(*args, **kwargs)

        self.constraint_mapping = ConstraintMapping()
        self.init_xschema()

    def init_xschema(self):
        self.refresh(output=False)

    def refresh(self, output=False):
        """Refresh the schema table

        Keyword Arguments:
            output {bool} -- if False, there will be no output, and maybe a little faster (default: {False})

        Raises:
            NonUniformTypeException -- Type constraint failed
            NotNullException -- NotNull constraint failed
            NotUniqueException -- Unique constraint failed

        Returns:
            dict -- dictionary of constraints
        """

        output_mapping = None
        if output:
            output_mapping = deepcopy(self.constraint_mapping)

        for record in self.all():
            for k, v in parse_record(record, yield_='type'):
                expected_type = self.constraint_mapping.type_.get(k, None)
                if expected_type and v is not expected_type:
                    if expected_type is str and v in (int, float):
                        v = str
                    else:
                        raise NonUniformTypeException('{} type is not {}'.format(v, expected_type))

                if output_mapping:
                    type_list = output_mapping.type_.get(k, [])
                    if isinstance(type_list, type):
                        type_list = [type_list]

                    if v not in type_list:
                        type_list.append(v)

                    output_mapping.type_[k] = type_list

            record = dict(parse_record(record, yield_='record'))
            is_null = self.constraint_mapping.not_null - set(record.keys())

            if len(is_null) > 0:
                raise NotNullException('{} is null'.format(list(is_null)))

        if output_mapping:
            for k, v in output_mapping.type_.items():
                if isinstance(v, list) and len(v) == 1:
                    output_mapping.type_[k] = v[0]

            return output_mapping.view()

    def insert(self, row, ensure=None, types=None):
        self._update_uniqueness(row)
        return super(XSchemaTable, self).insert(row, ensure, types)

    def insert_ignore(self, row, keys, ensure=None, types=None):
        self._update_uniqueness(row)
        return super(XSchemaTable, self).insert_ignore(row, keys, ensure, types)

    def insert_many(self, rows, chunk_size=1000, ensure=None, types=None):
        for row in rows:
            self._update_uniqueness(row)
        return super(XSchemaTable, self).insert_many(rows, chunk_size, ensure, types)

    def update(self, row, keys, ensure=None, types=None, return_count=False):
        self._update_uniqueness(row)
        return super(XSchemaTable, self).update(row, keys, ensure, types, return_count)

    def upsert(self, row, keys, ensure=None, types=None):
        self._update_uniqueness(row)
        return super(XSchemaTable, self).upsert(row, keys, ensure, types)

    def delete(self, *clauses, **filters):
        for record in self.find(*clauses, **filters):
            self._remove_uniqueness(record)

        try:
            return super(XSchemaTable, self).delete(*clauses, **filters)
        finally:
            self.refresh()

    @property
    def schema(self):
        """Get table's latest schema

        Returns:
            dict -- dictionary of constraints
        """

        return self.get_schema(refresh=True)

    @schema.setter
    def schema(self, schema_dict):
        """Reset and set a new schema

        Arguments:
            schema_dict {dict} -- dictionary of constraints or types
        """

        self.constraint_mapping = ConstraintMapping()
        self.update_schema(schema_dict)

    def get_schema(self, refresh=False):
        """Get table's schema, while providing an option to disable refreshing to allow faster getting of schema

        Keyword Arguments:
            refresh {bool} -- disable refreshing to allow faster getting of schema (default: {False})

        Returns:
            dict -- dictionary of constraints
        """

        if refresh:
            return self.refresh(output=True)
        else:
            return self.constraint_mapping.view()

    def update_schema(self, schema_dict):
        """Update the schema

        Arguments:
            schema_dict {dict} -- dictionary of constraints or types
        """

        self.constraint_mapping.update(schema_dict)

    def _sanitize_multiple(self, records):
        """Sanitizes records, e.g. from Excel spreadsheet

        Arguments:
            records {iterable} -- Iterable of records

        Returns:
            list -- List of records
        """

        def _records():
            for record in records:
                record_schema = dict(parse_record(record, yield_='type'))
                num_to_str = set()
                for k, v in record_schema.items():
                    expected_type = self.constraint_mapping.type_.get(k, None)
                    if expected_type and v is not expected_type:
                        if expected_type is str and v in (int, float):
                            # v = str
                            num_to_str.add(k)
                        else:
                            raise NonUniformTypeException('{} not in table schema {}'
                                                          .format(v, self.get_schema(refresh=False)))

                    self.update_schema(record_schema)

                record = dict(parse_record(record, yield_='type'))
                for k, v in record.items():
                    if k in num_to_str:
                        record[k] = str(v)

                yield record

        if bool(int(os.getenv('XSCHEMA_SANITIZE', '1'))):
            self.refresh()
            for c in self.get_schema(refresh=False).values():
                assert not isinstance(c.type_, list)

            # self.refresh()
            return list(_records())
        else:
            return records

    def _sanitize_one(self, record):
        return self._sanitize_multiple([record])[0]

    def _update_uniqueness(self, record_dict):
        for k, v in parse_record(record_dict, yield_='type'):
            if k in self.constraint_mapping.preexisting.keys():
                if v in self.constraint_mapping.preexisting[k]:
                    raise NotUniqueException('Duplicate {} for {} exists'.format(v, k))

                self.constraint_mapping.preexisting[k].add(v)

    def _remove_uniqueness(self, record_dict):
        for k, v in parse_record(record_dict, yield_='type'):
            if k in self.constraint_mapping.preexisting.keys():
                if v in self.constraint_mapping.preexisting[k]:
                    self.constraint_mapping.preexisting[k].remove(v)
