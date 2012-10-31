library avro_schema;

import 'dart:json';

class AvroTypeError {
  final String message;
  AvroTypeError(this.message);
}

class SchemaParseError {
  final String message;
  SchemaParseError(this.message);
}

final Map<String, Schema> _primitiveTypes = {
 'null': new AvroNull(),
 'boolean': new AvroBoolean(),
 'int': new AvroInt(),
 'long': new AvroLong(),
 'float': new AvroFloat(),
 'double': new AvroDouble(),
 'bytes': new AvroBytes(),
 'string': new AvroString()
};


/**
 * Represents an Avro schema. See
 * http://avro.apache.org/docs/current/spec.html
 * for more information.
 */
abstract class Schema {
  /**
   * Parses an Avro schema from its string representation, which is one of:
   * * a JSON string, naming a defined type;
   * * a JSON object, defining a schema; or
   * * a JSON array, representing a union of embedded types.
   * See http://avro.apache.org/docs/current/spec.html#schemas for more information.
   */
  factory Schema.parse(String json) {
    var obj = JSON.parse(json);
    if (obj is String) {
      if (_primitiveTypes.containsKey(obj)) {
        return _primitiveTypes[obj];
      } else {
        throw new AvroTypeError('Undefined type "$obj"');
      }
    } else if (obj is Map) {
      String typeName = obj['type'];
      if (_primitiveTypes.containsKey(typeName)) {
        return _primitiveTypes[typeName];
      }
      switch (typeName) {
        case 'record':
          return new Record(obj['name'], obj['namespace'], obj['fields']);
        default: throw new AvroTypeError('Undefined type "$typeName"');
      }
    } else if (obj is List) {
      throw new NotImplementedException();
    } else {
      throw new SchemaParseError('Expected JSON string, object, or array; got $json');
    }
  }
}

abstract class PrimitiveType implements Schema {}

class AvroNull implements PrimitiveType {
  static AvroNull _inst;
  factory AvroNull() => _inst != null ? _inst : (_inst = new AvroNull._internal());
  AvroNull._internal();
}
class AvroBoolean implements PrimitiveType {
  static AvroBoolean _inst;
  factory AvroBoolean() => _inst != null ? _inst : (_inst = new AvroBoolean._internal());
  AvroBoolean._internal();
}
class AvroInt implements PrimitiveType {
  static AvroInt _inst;
  factory AvroInt() => _inst != null ? _inst : (_inst = new AvroInt._internal());
  AvroInt._internal();
}
class AvroLong implements PrimitiveType {
  static AvroLong _inst;
  factory AvroLong() => _inst != null ? _inst : (_inst = new AvroLong._internal());
  AvroLong._internal();
}
class AvroFloat implements PrimitiveType {
  static AvroFloat _inst;
  factory AvroFloat() => _inst != null ? _inst : (_inst = new AvroFloat._internal());
  AvroFloat._internal();
}
class AvroDouble implements PrimitiveType {
  static AvroDouble _inst;
  factory AvroDouble() => _inst != null ? _inst : (_inst = new AvroDouble._internal());
  AvroDouble._internal();
}
class AvroBytes implements PrimitiveType {
  static AvroBytes _inst;
  factory AvroBytes() => _inst != null ? _inst : (_inst = new AvroBytes._internal());
  AvroBytes._internal();
}
class AvroString implements PrimitiveType {
  static AvroString _inst;
  factory AvroString() => _inst != null ? _inst : (_inst = new AvroString._internal());
  AvroString._internal();
}

class Record implements Schema {
  final String name;
  final String namespace;
  final List fields;
  Record(this.name, this.namespace, this.fields);
  String toString() => 'Record($name, $namespace, $fields)';
}
