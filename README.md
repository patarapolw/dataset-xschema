# dataset-xschema

External schema, i.e. not embedded in database file, for [dataset](https://github.com/pudo/dataset).

## Installation

- Clone the project from GitHub
- `poetry install PATH/TO/DATASET/XSCHEMA`
- This uses the [forked dataset](https://github.com/patarapolw/dataset), that supports Table class.

## Usage

>>> from dataset import dataset
>>> from dataset_xschema import XSchemaTable
>>> from datetime import datetime
>>> dataset.table_class = XSchemaTable
>>> db = TinyDB('foo.db')
>>> table = db['bar']
>>> table.schema = {
...     'record_id': int,
...     'modified': datetime
... }
>>> table.schema
{
    'record_id': Constraint(type_=int, unique=False, not_null=False),
    'modified': Constraint(type_=datetime.datetime, unique=False, not_null=False)
}

## Note

I don't use datetime, but a JSON-serializable datetime string (`datetime.isoformat()`), and to set `datetime`, you have to pass a `dateutil.parser.parse()`-parsable string.

## Advanced usage

Database schema is also settable via `Constraint` object.

```python
>>> from dataset_xschema import Constraint
>>> table.update_schema({
...     'user_id': Constraint(type_=int, unique=True, not_null=True)
... })
```

If you want to disable certain string sanitization features, like stripping spaces or checking if string can be converted to datetime, this can be done by setting environmental variables.

```
XSCHEMA_SANITIZE=0
XSCHEMA_DATETIME=0
```

## Related projects

- https://github.com/patarapolw/tinydb-constraint
