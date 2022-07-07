/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.junit.Assert.*;

@RunWith(value = Enclosed.class)
public class MyTestSchema {

  @RunWith(Parameterized.class)
  public static class TestFile {

    public enum TypeEntry {
      BOOL, BYTE, INT, LONG, FLOAT, RECORD, ENUM, ARRAY, MAP, DOUBLE, NULL
    }

    public enum TypeElement {
      VALID, NULL, NOVALID, EMPTY
    }

    private static Schema.Parser parser;
    private static TypeElement typeElement;
    private static TypeEntry typeEntry;
    private static File jsonFile;
    private static String expectedValue;
    private static boolean expectedNull;
    private static boolean expectedSchema;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays
          .asList(new Object[][] { { TypeElement.EMPTY, TypeEntry.BOOL }, { TypeElement.VALID, TypeEntry.BOOL },
              { TypeElement.VALID, TypeEntry.INT }, { TypeElement.VALID, TypeEntry.BYTE },
              { TypeElement.VALID, TypeEntry.LONG }, { TypeElement.VALID, TypeEntry.FLOAT },
              { TypeElement.VALID, TypeEntry.RECORD }, { TypeElement.VALID, TypeEntry.ENUM },
              { TypeElement.VALID, TypeEntry.ARRAY }, { TypeElement.VALID, TypeEntry.MAP },
              { TypeElement.NULL, TypeEntry.BOOL }, { TypeElement.NOVALID, TypeEntry.BOOL },

              // Pit
              { TypeElement.VALID, TypeEntry.DOUBLE }, { TypeElement.NOVALID, TypeEntry.DOUBLE },
              { TypeElement.VALID, TypeEntry.NULL }, { TypeElement.NOVALID, TypeEntry.NULL },

          });
    }

    public TestFile(TypeElement typeelement, TypeEntry typeentry) throws JSONException {
      parser = new Schema.Parser();
      typeElement = typeelement;
      typeEntry = typeentry;

      setUp();
      getOracle();
    }

    public static void setUp() throws JSONException {

      switch (typeElement) {

      case NULL:
        jsonFile = null;
        break;

      case NOVALID:
        jsonFile = new File("file.json");
        String invalidJson = "{Invalid string json}";

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(invalidJson);
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case EMPTY:
        jsonFile = new File("file.json");

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write("");
        } catch (Exception e) {
          e.printStackTrace();
        }

        break;

      case VALID:
        setUpValidFile();
        break;
      }

    }

    private static void setUpValidFile() throws JSONException {

      JSONObject jsonObject = new JSONObject();
      jsonFile = new File("file.json");

      switch (typeEntry) {

      case BOOL:
        jsonObject = new JSONObject();
        jsonObject.put("type", "boolean");
        jsonObject.put("value", true);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case BYTE:
        String byteStr = "byteStr";
        jsonObject = new JSONObject();
        jsonObject.put("type", "bytes");
        jsonObject.put("value", byteStr.getBytes());

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case INT:
        jsonObject = new JSONObject();
        jsonObject.put("type", "int");
        jsonObject.put("value", 0);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case LONG:
        jsonObject = new JSONObject();
        jsonObject.put("type", "long");
        jsonObject.put("value", 0L);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }

        break;

      case FLOAT:
        float num = 1;
        jsonObject = new JSONObject();
        jsonObject.put("type", "float");
        jsonObject.put("value", num);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }

        break;

      case DOUBLE:
        jsonObject = new JSONObject();
        jsonObject.put("type", "double");
        jsonObject.put("value", "double");

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case NULL:
        jsonObject = new JSONObject();
        jsonObject.put("type", "null");
        jsonObject.put("value", "nullprova");

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case ENUM:

        String[] enumValue = { "A", "B", "C" };
        jsonObject.put("type", "enum");
        jsonObject.put("name", "enumName");
        jsonObject.put("doc", "this is a enum");
        jsonObject.put("symbols", enumValue);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case ARRAY:
        String[] arrayValue = { "A", "B", "C" };
        jsonObject.put("type", "array");
        jsonObject.put("items", "string");
        jsonObject.put("value", arrayValue);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }
        break;

      case MAP:
        HashMap<String, String> map = new HashMap<>();
        map.put("test", "prova");

        jsonObject.put("type", "map");
        jsonObject.put("values", "string");
        jsonObject.put("value", map);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }

        break;

      case RECORD:
        JSONObject field1 = new JSONObject();
        JSONObject field2 = new JSONObject();

        field1.put("type", "int");
        field1.put("name", "name1");
        field1.put("value", 0);

        field2.put("type", "int");
        field2.put("name", "name2");
        field2.put("value", 10);

        JSONObject[] fields = { field1, field2 };

        jsonObject.put("type", "record");
        jsonObject.put("name", "recordName");
        jsonObject.put("fields", fields);

        try (PrintWriter out = new PrintWriter(new FileWriter("file.json"))) {
          out.write(jsonObject.toString());
        } catch (Exception e) {
          e.printStackTrace();
        }

        break;
      }

      expectedValue = jsonObject.toString();

    }

    @Test
    public void test() throws IOException {

      try {
        Schema schema = parser.parse(jsonFile);
        char[] expSort = expectedValue.toCharArray();
        char[] attualSort = schema.toString().toCharArray();
        Arrays.sort(expSort);
        Arrays.sort(attualSort);

        assertArrayEquals(expSort, attualSort);
      } catch (NullPointerException e) {
        assertTrue(expectedNull);
      } catch (SchemaParseException e) {
        assertTrue(expectedSchema);
      }

    }

    public void getOracle() {
      if (typeElement.equals(TypeElement.NULL))
        expectedNull = true;
      if (typeElement.equals(TypeElement.NOVALID) || typeElement.equals(TypeElement.EMPTY))
        expectedSchema = true;
    }

    @After
    public void deleteFile() {
      File file = new File("file.json");
      file.delete();
    }

  }

  @RunWith(Parameterized.class)
  public static class TestRecord {

    public enum TypeElement {
      VALID, NULL, NOVALID, EMPTY
    }

    private static TypeElement typeRecord;
    private static TypeElement nameRecord;
    private static TypeElementFiel typefield;
    private static JSONObject objectToConvert;
    private static Schema.Parser parser;
    private static boolean expectedEXception;

    public enum TypeElementFiel {
      ONE_FIELD, EMPTY, MORE_FIELDS, NO_VALID
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {
          /* TYPE, NAME, FIELD */
          { TypeElement.VALID, TypeElement.VALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.VALID, TypeElement.VALID, TypeElementFiel.EMPTY },
          { TypeElement.VALID, TypeElement.VALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.VALID, TypeElement.VALID, TypeElementFiel.NO_VALID },
          { TypeElement.NOVALID, TypeElement.VALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.NOVALID, TypeElement.VALID, TypeElementFiel.EMPTY },
          { TypeElement.NOVALID, TypeElement.VALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.NOVALID, TypeElement.VALID, TypeElementFiel.NO_VALID },
          { TypeElement.EMPTY, TypeElement.VALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.EMPTY, TypeElement.VALID, TypeElementFiel.EMPTY },
          { TypeElement.EMPTY, TypeElement.VALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.EMPTY, TypeElement.VALID, TypeElementFiel.NO_VALID },
          { TypeElement.VALID, TypeElement.NOVALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.VALID, TypeElement.NOVALID, TypeElementFiel.EMPTY },
          { TypeElement.VALID, TypeElement.NOVALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.VALID, TypeElement.NOVALID, TypeElementFiel.NO_VALID },
          { TypeElement.NOVALID, TypeElement.NOVALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.NOVALID, TypeElement.NOVALID, TypeElementFiel.EMPTY },
          { TypeElement.NOVALID, TypeElement.NOVALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.NOVALID, TypeElement.NOVALID, TypeElementFiel.NO_VALID },
          { TypeElement.EMPTY, TypeElement.NOVALID, TypeElementFiel.ONE_FIELD },
          { TypeElement.EMPTY, TypeElement.NOVALID, TypeElementFiel.EMPTY },
          { TypeElement.EMPTY, TypeElement.NOVALID, TypeElementFiel.MORE_FIELDS },
          { TypeElement.EMPTY, TypeElement.NOVALID, TypeElementFiel.NO_VALID },
          { TypeElement.VALID, TypeElement.EMPTY, TypeElementFiel.ONE_FIELD },
          { TypeElement.VALID, TypeElement.EMPTY, TypeElementFiel.EMPTY },
          { TypeElement.VALID, TypeElement.EMPTY, TypeElementFiel.MORE_FIELDS },
          { TypeElement.VALID, TypeElement.EMPTY, TypeElementFiel.NO_VALID },
          { TypeElement.NOVALID, TypeElement.EMPTY, TypeElementFiel.ONE_FIELD },
          { TypeElement.NOVALID, TypeElement.EMPTY, TypeElementFiel.EMPTY },
          { TypeElement.NOVALID, TypeElement.EMPTY, TypeElementFiel.MORE_FIELDS },
          { TypeElement.NOVALID, TypeElement.EMPTY, TypeElementFiel.NO_VALID },
          { TypeElement.EMPTY, TypeElement.EMPTY, TypeElementFiel.ONE_FIELD },
          { TypeElement.EMPTY, TypeElement.EMPTY, TypeElementFiel.EMPTY },
          { TypeElement.EMPTY, TypeElement.EMPTY, TypeElementFiel.MORE_FIELDS },
          { TypeElement.EMPTY, TypeElement.EMPTY, TypeElementFiel.NO_VALID },

      });
    }

    public TestRecord(TypeElement typerecord, TypeElement typename, TypeElementFiel typeField) throws JSONException {
      typeRecord = typerecord;
      nameRecord = typename;
      typefield = typeField;

      sutUp();
      getOracle();
    }

    private void sutUp() throws JSONException {
      parser = new Schema.Parser();
      objectToConvert = new JSONObject();
      switch (typeRecord) {

      case VALID:
        objectToConvert.put("type", "record");
        break;

      case NOVALID:
        break;

      case EMPTY:
        objectToConvert.put("type", "");
        break;

      case NULL:
        objectToConvert.put("type", (String) null);
        break;
      }

      switch (nameRecord) {

      case VALID:
        objectToConvert.put("name", "namerecord");
        break;

      case NOVALID:
        break;

      case NULL:
        objectToConvert.put("name", (String) null);
        break;

      case EMPTY:
        objectToConvert.put("name", "");
        break;

      }

      switch (typefield) {

      case ONE_FIELD:
        JSONObject field1 = new JSONObject();

        field1.put("type", "int");
        field1.put("name", "name1");
        field1.put("value", 0);

        JSONObject[] fields = { field1 };

        objectToConvert.put("fields", fields);

        break;

      case MORE_FIELDS:
        JSONObject field = new JSONObject();
        JSONObject field2 = new JSONObject();

        field.put("type", "int");
        field.put("name", "name1");
        field.put("value", 0);

        field2.put("type", "float");
        field2.put("name", "name2");
        field2.put("value", 10);

        JSONObject[] fieldsList = { field, field2 };

        objectToConvert.put("fields", fieldsList);
        break;

      case NO_VALID:
        JSONObject f = new JSONObject();

        f.put("type", "int");
        f.put("value", 0);

        JSONObject[] fieldslist = { f };
        objectToConvert.put("fields", fieldslist);

        break;

      case EMPTY:
        JSONObject[] fieldList = {};
        objectToConvert.put("fields", fieldList);
        break;

      }

    }

    @Test
    public void testSingleWrite() {

      try {
        String json = objectToConvert.toString();
        Schema schema = parser.parse(json);

        char[] expSort = json.toCharArray();
        char[] attualSort = schema.toString().toCharArray();
        Arrays.sort(expSort);
        Arrays.sort(attualSort);

        assertArrayEquals(expSort, attualSort);
      } catch (SchemaParseException e) {
        assertTrue(expectedEXception);
      }

    }

    private void getOracle() {
      if (!nameRecord.equals(TypeElement.VALID) || !typeRecord.equals(TypeElement.VALID)
          || typefield.equals(TypeElementFiel.NO_VALID))
        expectedEXception = true;
    }

  }

  public static class WhiteBoxTestRemove {

    private Schema recordSchema;

    public void setUP() {
      recordSchema = Schema.createRecord("RecordTest", "This is a record", "Record", true);
    }

    @Test
    public void test1() {
      setUP();
      List<Schema.Field> fields1 = new ArrayList<>();
      Schema.Field field = new Schema.Field("field", Schema.create(Schema.Type.STRING));
      fields1.add(field);
      recordSchema.setFields(fields1);
      assertEquals(recordSchema.getFields().size(), 1);
    }

    @Test(expected = AvroRuntimeException.class)
    public void test2() {
      setUP();
      List<Schema.Field> fields = new ArrayList<>();
      Schema.Field field = new Schema.Field("field", Schema.create(Schema.Type.STRING));
      fields.add(field);
      recordSchema.setFields(fields);
      recordSchema.setFields(fields);
    }

    @Test(expected = AvroRuntimeException.class)
    public void test3() {
      setUP();
      List<Schema.Field> fields = new ArrayList<>();
      Schema.Field field = new Schema.Field("field", Schema.create(Schema.Type.STRING));
      fields.add(field);
      fields.add(field);
      recordSchema.setFields(fields);
    }

    @Test
    public void test4() {
      String nameStr = "name";
      String space = "";
      Schema.Name name = new Schema.Name(nameStr, space);

      assertEquals(name.toString(), nameStr);
    }

    @Test(expected = SchemaParseException.class)
    public void test5() {
      String nameStr = "";
      String space = "";
      Schema.Name name = new Schema.Name(nameStr, space);

    }

    @Test(expected = SchemaParseException.class)
    public void test6() {
      String nameStr = "1nome";
      String space = "";
      Schema.Name name = new Schema.Name(nameStr, space);
    }

    @Test(expected = SchemaParseException.class)
    public void test7() {
      String nameStr = "no!me";
      String space = "";
      Schema.Name name = new Schema.Name(nameStr, space);
    }

  }

}
