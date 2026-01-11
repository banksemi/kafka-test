from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import BaseModel

value_schema_str = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age",  "type": "int"},
    {"name": "ag2e",  "type": "int"},
    {"name": "email", "type": ["null", "string"], "default": null} 
  ]
}
"""

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

class User(BaseModel):
    name: str
    age: int
    email: str | None = None


user = User(name="user_name", age=30, email="test@test.com")
result = avro_serializer(user.model_dump(), SerializationContext('users-avro', MessageField.VALUE))
user.name = "123123"
result = avro_serializer(user.model_dump(), SerializationContext('users-avro', MessageField.VALUE))
exit(1)
print(result, type(result))
