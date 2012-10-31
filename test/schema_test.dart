library avro_schema_test;

import 'package:unittest/vm_config.dart';
import 'package:unittest/unittest.dart';
import '../lib/schema.dart';

main() {
  useVmConfiguration();

  testSchema();
}

void testSchema() {
  group('Schema.parse', () {
    test('throws on undefined type reference', () {
      expect(() => new Schema.parse('"mytype"'), throwsA(predicate((AvroTypeError e) => e.message == 'Undefined type "mytype"')));
    });
    test('throws on undefined type name', () {
      expect(() => new Schema.parse('{"type":"mytype"}'), throwsA(predicate((AvroTypeError e) => e.message == 'Undefined type "mytype"')));
    });
    group('primitives', () {
      test('null', () {
        expect(new Schema.parse('"null"'), new AvroNull());
        expect(new Schema.parse('{"type":"null"}'), new AvroNull());
      });
      test('boolean', () {
        expect(new Schema.parse('"boolean"'), new AvroBoolean());
        expect(new Schema.parse('{"type":"boolean"}'), new AvroBoolean());
      });
      test('int', () {
        expect(new Schema.parse('"int"'), new AvroInt());
        expect(new Schema.parse('{"type":"int"}'), new AvroInt());
      });
      test('long', () {
        expect(new Schema.parse('"long"'), new AvroLong());
        expect(new Schema.parse('{"type":"long"}'), new AvroLong());
      });
      test('float', () {
        expect(new Schema.parse('"float"'), new AvroFloat());
        expect(new Schema.parse('{"type":"float"}'), new AvroFloat());
      });
      test('double', () {
        expect(new Schema.parse('"double"'), new AvroDouble());
        expect(new Schema.parse('{"type":"double"}'), new AvroDouble());
      });
      test('bytes', () {
        expect(new Schema.parse('"bytes"'), new AvroBytes());
        expect(new Schema.parse('{"type":"bytes"}'), new AvroBytes());
      });
      test('string', () {
        expect(new Schema.parse('"string"'), new AvroString());
        expect(new Schema.parse('{"type":"string"}'), new AvroString());
      });
    });
    group('records', () {
      test('empty record', () {
        var s = new Schema.parse('{"type":"record", "name":"EmptyRecord", "fields":[]}');
        expect(s.name, 'EmptyRecord');
        expect(s.namespace, null);
        expect(s.fields.isEmpty, isTrue);
      });
    });
    group('unions', () {
      test('not implemented', () {
        expect(() => new Schema.parse('["string", "int"]'), throwsA(predicate((e) => e is NotImplementedException)));
      });
    });
  });
}
