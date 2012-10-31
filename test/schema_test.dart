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
      void expectRecord(String json, Record expected) {
        expect(new Schema.parse(json), expected);
      }
      test('empty record', () {
        expectRecord('{"type":"record", "name":"EmptyRecord", "fields":[]}', new Record('EmptyRecord', null, []));
      });
      test('empty record with namespace', () {
        expectRecord('{"type":"record", "name":"EmptyRecord", "namespace":"a.b", "fields":[]}', new Record('EmptyRecord', 'a.b', []));
      });
      test('record with null', () {
        expectRecord('{"type":"record", "name":"RecordWithNull", "fields":[{"type":"null", "name":"nullField"}]}', new Record('RecordWithNull', null, [new Field('nullField', new AvroNull(), null, 0)]));
      });
      test('record with null and int', () {
        expectRecord('{"type":"record", "name":"RecordWithNullAndInt", "fields":[{"type":"null", "name":"nullField"}, {"type":"int", "name":"intField"}]}', new Record('RecordWithNullAndInt', null, [new Field('nullField', new AvroNull(), null, 0), new Field('intField', new AvroInt(), null, 1)]));
      });
      test('record with sub-record', () {
        expectRecord('{"type":"record", "name":"RecordWithSubrecord", "fields":[{"type":{"type":"record","fields":[]}, "name":"subrecordField"}]}', new Record('RecordWithSubrecord', null, [new Field('subrecordField', new Record(null, null, []), null, 0)]));
      });
      test('throws on undefined field type', () {
        expect(() => new Schema.parse('{"type":"record", "name":"RecordWithUndefinedField", "fields":[{"type":"doesntexist", "name":"doesntexistField"}]}'), throwsA(predicate((AvroTypeError e) => e.message == 'Undefined type "doesntexist"')));
      });
    });
    group('unions', () {
      test('union with no branches', () {
        expect(new Schema.parse('[]'), new Union([]));
      });
      test('union with one branch', () {
        expect(new Schema.parse('["string"]'), new Union([new AvroString()]));
      });
      test('union with two branches', () {
        expect(new Schema.parse('["string", "int"]'), new Union([new AvroString(), new AvroInt()]));
      });
    });
  });
}
