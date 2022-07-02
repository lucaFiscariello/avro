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
package org.apache.avro.generic;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.path.TracingNullPointException;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class MyTestGenericDatumWriter {

  @RunWith(Parameterized.class)
  public static class TestWrite {

    private enum TypeWrite {
      ONE, MULTIPLE, NONE
    }

    private enum TypeValueWrite {
      VALID, NOVALID
    }

    private TypeValueWrite typeValueWrite;
    private TypeWrite typeWrite;
    private GenericArray<Integer> a;
    private GenericDatumWriter<GenericArray<Integer>> w;
    private Schema s;
    private boolean expectedNullException;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] { { TypeWrite.ONE, TypeValueWrite.VALID },
          { TypeWrite.MULTIPLE, TypeValueWrite.VALID }, { TypeWrite.NONE, TypeValueWrite.VALID },
          { TypeWrite.ONE, TypeValueWrite.NOVALID }, { TypeWrite.MULTIPLE, TypeValueWrite.NOVALID }, });
    }

    public void setUp() {
      String json = "{\"type\": \"array\", \"items\": \"int\" }";
      s = new Schema.Parser().parse(json);
      w = new GenericDatumWriter<>(s);

      switch (typeWrite) {

      case ONE:
        a = new GenericData.Array<>(1, s);
        sutUpOne();
        break;

      case MULTIPLE:
        a = new GenericData.Array<>(3, s);
        supUpMultiple();
        break;

      case NONE:
        a = new GenericData.Array<>(0, s);
        break;
      }

    }

    private void supUpMultiple() {
      switch (typeValueWrite) {

      case VALID:
        a.add(1);
        a.add(2);
        a.add(3);
        break;

      case NOVALID:
        a.add(null);
        a.add(1);
        a.add(2);
        break;
      }
    }

    private void sutUpOne() {
      switch (typeValueWrite) {

      case VALID:
        a.add(1);
        break;

      case NOVALID:
        a.add(null);
        break;
      }
    }

    public TestWrite(TypeWrite typewrite, TypeValueWrite typeValuewrite) {
      typeValueWrite = typeValuewrite;
      typeWrite = typewrite;
      setUp();
      getOracle();
    }

    @Test
    public void testWrite() throws IOException {

      try {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
        w.write(a, e);
        e.flush();

        Object o = new GenericDatumReader<GenericRecord>(s).read(null,
            DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
        assertEquals(a, o);
      } catch (NullPointerException e) {
        assertTrue(expectedNullException);
      }

    }

    private void getOracle() {
      expectedNullException = typeValueWrite.equals(TypeValueWrite.NOVALID);
    }

  }

  @RunWith(MockitoJUnitRunner.class)
  public static class TestBlackBox {

    private CountDownLatch sizeWrittenSignal;
    private CountDownLatch eltAddedSignal;
    private CountDownLatch finalTime;
    private GenericDatumWriter<GenericArray<Integer>> w;
    private GenericDatumWriter<Map<String, Integer>> wm;
    private ByteArrayOutputStream bao;
    private GenericArray<Integer> a;
    private Map<String, Integer> m;

    @Mock
    Encoder e;

    public void setUp(Schema s) throws IOException {

      bao = new ByteArrayOutputStream();
      sizeWrittenSignal = new CountDownLatch(1);
      eltAddedSignal = new CountDownLatch(1);
      finalTime = new CountDownLatch(1);

      e = mock(Encoder.class);
      doAnswer(invocation -> {
        EncoderFactory.get().directBinaryEncoder(bao, null).writeArrayStart();
        sizeWrittenSignal.countDown();

        try {
          eltAddedSignal.await();
        } catch (InterruptedException e) {
          // ignore
        }
        return null;
      }).when(e).writeArrayStart();

    }

    public void setUpArray() throws IOException {
      String json = "{\"type\": \"array\", \"items\": \"int\" }";
      Schema s = new Schema.Parser().parse(json);
      a = new GenericData.Array<>(1, s);
      w = new GenericDatumWriter<>(s);
      setUp(s);
    }

    public void setUpMap() throws IOException {
      String json = "{\"type\": \"map\", \"values\": \"int\" }";
      Schema s = new Schema.Parser().parse(json);
      m = new HashMap<>();
      wm = new GenericDatumWriter<>(s);
      setUp(s);
    }

    @Test
    public void testArrayConcurrentModification() throws Exception {

      setUpArray();
      AtomicBoolean throwException = new AtomicBoolean();

      new Thread(() -> {
        try {
          w.write(a, e);
        } catch (ConcurrentModificationException | IOException ex) {
          throwException.set(true);
          finalTime.countDown();
        }
      }).start();

      sizeWrittenSignal.await();
      a.add(10);
      eltAddedSignal.countDown();

      finalTime.await();
      assertTrue(throwException.get());
    }

    @Test
    public void testMapConcurrentModification() throws Exception {

      setUpMap();
      AtomicBoolean throwException = new AtomicBoolean();

      new Thread(() -> {
        try {
          sizeWrittenSignal.await();
          wm.write(m, e);
          eltAddedSignal.countDown();
        } catch (ConcurrentModificationException | IOException ex) {
          throwException.set(true);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }).start();

      m.put("key", 10);
      sizeWrittenSignal.countDown();
      eltAddedSignal.await();
      assertFalse(throwException.get());
    }

  }

  @RunWith(MockitoJUnitRunner.class)
  public static class TestWhiteBox {

    @Test
    public void test1() throws Exception {
      String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": [" + "{ \"name\": \"f1\", \"type\": \"long\" }"
          + "]}";
      LogicalType logical = new LogicalType("Record");

      Schema s = new Schema.Parser().parse(json);
      logical.addToSchema(s);

      GenericRecord r = new GenericData.Record(s);
      r.put("f1", 100L);
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(s);
      Encoder e = EncoderFactory.get().jsonEncoder(s, bao);

      GenericDatumWriter<GenericRecord> wSpy = spy(w);
      wSpy.write(s, r, e);
      e.flush();

      Object o = new GenericDatumReader<GenericRecord>(s).read(null,
          DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
      assertEquals(r, o);

      Mockito.verify(wSpy).convert(any(), any(), any(), any());

    }

    @Test
    public void test2() throws Exception {

      Schema s = null;
      String nameRecord = "r.f1";

      try {
        String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": ["
            + "{ \"name\": \"f1\", \"type\": \"long\" }" + "]}";
        LogicalType logical = new LogicalType("Record");

        s = new Schema.Parser().parse(json);
        logical.addToSchema(s);

        GenericRecord r = new GenericData.Record(s);
        r.put("f1", null);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(s);
        Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
        w.write(s, r, e);
        e.flush();
      } catch (TracingNullPointException e) {
        assertTrue(e.summarize(s).toString().contains(nameRecord));
      }

    }

    @Test(expected = NullPointerException.class)
    public void test3() throws IOException {
      String json = "{\"type\": \"map\", \"values\": \"int\" }";
      Schema s = new Schema.Parser().parse(json);
      GenericDatumWriter<Map<String, String>> w = new GenericDatumWriter<>(s);
      HashMap<String, String> m = new HashMap<>();
      m.put(null, "invalud");

      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
      w.write(m, e);
    }

    @Test
    public void test4() {
      Schema s = mock(Schema.class);
      LogicalType logical = mock((LogicalType.class));
      Object datum = mock(Object.class);

      GenericDatumWriter<Map<String, String>> w = new GenericDatumWriter<>(s);

      Object obj = w.convert(s, logical, null, datum);
      assertNotNull(obj);
    }

    @Test(expected = AvroRuntimeException.class)
    public void test5() {
      String json = "{\"type\": \"map\", \"values\": \"int\" }";
      Schema s = new Schema.Parser().parse(json);
      GenericDatumWriter<GenericData.Array<Integer>> w = new GenericDatumWriter<>(s);
      GenericData.Array<Integer> datum = new GenericData.Array<>(1, s);
      datum.add(2);

      LogicalType logical = new LogicalType("Array");
      Object obj = w.convert(s, logical, null, datum);

    }

    @Test(expected = IllegalArgumentException.class)
    public void test6() {
      String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": [" + "{ \"name\": \"f1\", \"type\": \"long\" }"
          + "]}";

      Schema s = new Schema.Parser().parse(json);
      GenericRecord datum = new GenericData.Record(s);

      GenericDatumWriter<GenericData.Record> w = new GenericDatumWriter<>(s);
      Conversion<GenericData.Record> conversion = mock(Conversion.class);

      Object obj = w.convert(s, null, conversion, datum);
    }

  }
}
