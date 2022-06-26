from schema_registry.client import SchemaRegistryClient, schema


_schema = schema.AvroSchema({
    "type": "record",
    "name": "user",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age",  "type": "int"}
    ]
})

schema_registry = SchemaRegistryClient("http://schema-registry-host:8081")
my_schema = schema_registry.register("yourSubjectName", _schema)