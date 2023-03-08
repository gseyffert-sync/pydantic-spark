from typing import List, Tuple
from collections import defaultdict

from pydantic import BaseModel


class SparkBase(BaseModel):
    """This is base pydantic class that will add some methods"""

    @classmethod
    def spark_schema(cls) -> dict:
        """Return the avro schema for the pydantic class"""
        schema = cls.schema()
        return cls._spark_schema(schema)

    @staticmethod
    def _spark_schema(schema: dict) -> dict:
        """Return the spark schema for the given pydantic schema"""
        classes_seen = {}

        def get_definition(ref: str, schema: dict):
            id = ref.replace("#/definitions/", "")
            d = schema.get("definitions", {}).get(id)
            if d is None:
                raise RuntimeError(f"Definition {id} does not exist")
            return d

        def get_type_of_definition(ref: str, schema: dict):
            """Reading definition of base schema for nested structs"""
            d = get_definition(ref, schema)

            if "enum" in d:
                enum_type = d.get("type")
                if enum_type == "string":
                    return "string"
                elif enum_type == "number":
                    return "double"
                elif enum_type == "integer":
                    return "long"
                else:
                    raise RuntimeError(f"Unknown enum type: {enum_type}")
            else:
                return {
                    "type": "struct",
                    "fields": get_fields(d),
                }

        def get_type(value: dict) -> Tuple[str, dict]:
            """Returns a type of single field"""
            t = value.get("type")
            if not t:
                t = 'list' if value.get('anyOf') else None
            f = value.get("format")
            r = value.get("$ref")
            a = value.get("additionalProperties")
            metadata = {}
            if "default" in value:
                metadata["default"] = value.get("default")
            if r is not None:
                class_name = r.replace("#/definitions/", "")
                match classes_seen.get(class_name):
                    case None:
                        # Make a record of the fact that we HAVE seen this class name,
                        #  but have not yet fully resolved its type
                        classes_seen[class_name] = "in progress"
                        spark_type = get_type_of_definition(r, schema)
                        classes_seen[class_name] = spark_type

                    case "in progress":
                        # If we have seen this class name, but are still in the process of fully
                        #  resolving its type, this must mean that we have a recursive type
                        #  definition and should abort the recursion with some generic type
                        spark_type = {
                            "type": {
                                "type": "map",
                                "keyType": "string",
                                "valueType": "string",
                                "valueContainsNull": True,
                            }
                        }
                        classes_seen[class_name] = spark_type

                    case _:
                        spark_type = classes_seen[class_name]

            elif t == "array":
                items = value.get("items")
                tn, metadata = get_type(items)
                spark_type = {
                    "type": "array",
                    "elementType": tn,
                    "containsNull": True,
                }
            elif t == "string" and f == "date-time":
                spark_type = "timestamp"
            elif t == "string" and f == "date":
                spark_type = "date"
            # elif t == "string" and f == "time":  # FIXME: time type in spark does not exist
            #     spark_type = {
            #         "type": "long",
            #         "logicalType": "time-micros",
            #     }
            elif t == "string" and f == "uuid":
                spark_type = "string"
                metadata["logicalType"] = "uuid"
            elif t == "string":
                spark_type = "string"
            elif t == "number":
                spark_type = "double"
            elif t == "integer":
                # integer in python can be a long
                spark_type = "long"
            elif t == "boolean":
                spark_type = "boolean"
            elif t == 'list':
                spark_type = "string"
            elif t == "object":
                if a is None:
                    value_type = "string"
                else:
                    value_type, m = get_type(a)
                # if isinstance(value_type, dict) and len(value_type) == 1:
                # value_type = value_type.get("type")
                spark_type = {"keyType": "string", "type": "map", "valueContainsNull": True, "valueType": value_type}
            else:
                raise NotImplementedError(
                    f"Type '{t}' not support yet, "
                    f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
                )
            return spark_type, metadata

        def get_fields(s: dict) -> List[dict]:
            """Return a list of fields of a struct"""
            fields = []

            required = s.get("required", [])
            for key, value in s.get("properties", {}).items():
                p_class = s.get("title")

                # This means we have a Union type.
                if anyof := value.get('anyOf'):
                    first_value = anyof[0]
                    first_subtype, first_metadata = get_type(first_value)

                    match first_subtype:
                        # For a union of structs, compute the schema for the various sub-types and merge their
                        #  fields together
                        case "struct":
                            subtype_fields = defaultdict(lambda: [])
                            for v in anyof:
                                spark_type, metadata = get_type(v)
                                spark_type_fields = spark_type.get("fields")
                                # Coalesce subfields with the same name into 1 collection...
                                for field in spark_type_fields:
                                    subtype_fields[field["name"]].append(field)

                            unique = []
                            for name, subfields in subtype_fields.items():
                                first_field = subfields[0]
                                if len(subfields) == len(anyof):
                                    nullable = all(field["nullable"] for field in subfields)
                                    first_field["nullable"] = nullable
                                else:
                                    # This means that this field wasn't encountered in all of our sub-models, and therefore
                                    #  must be nullable in our final union'd type
                                    first_field["nullable"] = True

                                first_field["metadata"]["parentClass"] = p_class
                                unique.append(first_field)

                            spark_type["fields"] = unique

                        # For the time being, for unions of e.g. Primitives, just take the first type
                        case _:
                            spark_type, metadata = first_subtype, first_metadata

                else:
                    spark_type, metadata = get_type(value)

                metadata["parentClass"] = p_class
                struct_field = {
                    "name": key,
                    "nullable": "default" not in metadata and key not in required,
                    "metadata": metadata,
                    "type": spark_type,
                }
                fields.append(struct_field)
            return fields

        fields = get_fields(schema)

        return {"fields": fields, "type": "struct"}
