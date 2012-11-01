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

  Schema parsedSchema() => _parse(JSON.parse(json));

  Schema _parse(obj, [String inNamespace = null]) {
    if (obj is String) {
      var typeName = obj;
      if (_primitiveTypes.containsKey(typeName)) {
        return _primitiveTypes[typeName];
      } else if (_typeScope.containsType(typeName, inNamespace)) {
        return _typeScope.lookupType(typeName, inNamespace);
      } else {
        throw new AvroTypeError('Undefined type "$typeName"');
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
      case 'enum':
          var e = new Enum(obj['name'], obj['namespace'], obj['symbols']);
          _typeScope.addType(e.name, e.namespace, e);
          return e;
        case 'array':
          return new ArraySchema(_parse(obj['items']));
        default: throw new AvroTypeError('Undefined type "$typeName"');
      }
    } else if (obj is List) {
      return new Union(obj.map((branch) => _parse(branch, inNamespace)));
    } else {
      throw new SchemaParseError('Expected JSON string, object, or array; got $json');
    }
  }

  List<Field> _fieldListFromJsonArray(List rawFields, String inNamespace) {
    var fields = [];
    for (int i = 0; i < rawFields.length; i++) {
      var f = rawFields[i];
      fields.add(new Field(f['name'], _parse(f['type'], inNamespace), f['default'], i));
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

  bool containsType(String typeIdentifier, String relativeToNamespace) =>
    lookupType(typeIdentifier, relativeToNamespace) != null;
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
  // TODO: hashCode
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

class Enum implements Schema {
  final String name;
  final String namespace;
  final List<String> symbols;
  Enum(this.name, this.namespace, this.symbols);

  bool operator==(o) => o is Enum && this.name == o.name && this.namespace == o.namespace && enumSymbolsEqual(this.symbols, o.symbols);
  // TODO: hashCode
  String toString() => 'Enum($name, $namespace, $symbols)';

  static enumSymbolsEqual(List<Schema> s1, List<Schema> s2) {
    if (s1.length != s2.length) return false;
    for (int i = 0; i < s1.length; i++) {
      if (s1[i] != s2[i]) return false;
    }
    return true;    
  }
}

class ArraySchema implements Schema {
  final Schema elementType;
  ArraySchema(this.elementType);

  bool operator==(o) => o is ArraySchema && this.elementType == o.elementType;
  // TODO: hashCode
  String toString() => 'ArraySchema($elementType)';
}

class Union implements Schema {
  final List<Schema> branches;
  Union(this.branches);

  bool operator==(o) => o is Union && unionBranchesEqual(this.branches, o.branches);
  // TODO: hashCode
  String toString() => 'Union($branches)';

  static unionBranchesEqual(List<Schema> bs1, List<Schema> bs2) {
    if (bs1.length != bs2.length) return false;
    for (int i = 0; i < bs1.length; i++) {
      if (bs1[i] != bs2[i]) return false;
    }
    return true;    
  }
}
