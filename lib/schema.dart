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

class _SchemaParser {
  final String json;

  _SchemaParser(this.json);
  _TypeScope _typeScope = new _TypeScope();

  Schema parsedSchema() {
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
          var record = new Record(obj['name'], obj['namespace'], _fieldListFromJsonArray(obj['fields'], obj['namespace']));
          _typeScope.addType(record.name, record.namespace, record);
          return record;
        default: throw new AvroTypeError('Undefined type "$typeName"');
      }
    } else if (obj is List) {
      throw new NotImplementedException();
    } else {
      throw new SchemaParseError('Expected JSON string, object, or array; got $json');
    }
  }

  List<Field> _fieldListFromJsonArray(List rawFields, String inNamespace) {
    var fields = [];
    for (int i = 0; i < rawFields.length; i++) {
      var f = rawFields[i];
      fields.add(new Field(f['name'], _typeScope.lookupType(f['type'], inNamespace), f['default'], i));
    }
    return fields;
  }
}

class _TypeScope {
  Map<String, Schema> _definedTypes = new Map.from(_primitiveTypes);

  void addType(String typeName, String namespace, Schema schema) {
    if (typeName != null) {
      var qualifiedName = namespace != null ? '$namespace.$typeName' : typeName;
      _definedTypes[qualifiedName] = schema;
    }
  }

  Schema lookupType(String typeIdentifier, String relativeToNamespace) {
    // TODO: look up relative to namespace
    if (_definedTypes.containsKey(typeIdentifier)) {
      return _definedTypes[typeIdentifier];
    } else {
      throw new AvroTypeError('Undefined type "$typeIdentifier"');
    }
  }
}


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
    return new _SchemaParser(json).parsedSchema();
  }
}

class _DefinedTypeReference implements Schema {
  final String definedTypeName;
  _DefinedTypeReference(this.definedTypeName);
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
  final List<Field> fields;
  Record(this.name, this.namespace, this.fields);
  String toString() => 'Record($name, $namespace, $fields)';
  bool operator==(o) => o is Record && this.name == o.name && this.namespace == o.namespace && Field.fieldListsEqual(this.fields, o.fields);
}

class Field {
  final String name;
  final Schema schema;
  final Object defaultValue;
  final int pos;
  Field(this.name, this.schema, this.defaultValue, this.pos);

  bool operator==(o) => o is Field && this.name == o.name && this.schema == o.schema && JSON.stringify(this.defaultValue) == JSON.stringify(o.defaultValue) && this.pos == o.pos;
  // TODO: hashCode
  String toString() => 'Field($name, $schema, $defaultValue, $pos)';

  static fieldListsEqual(List<Field> fs1, List<Field> fs2) {
    if (fs1.length != fs2.length) return false;
    for (int i = 0; i < fs1.length; i++) {
      if (fs1[i] != fs2[i]) return false;
    }
    return true;
  }
}
