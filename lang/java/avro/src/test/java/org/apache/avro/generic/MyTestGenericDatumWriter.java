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

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.jmock.auto.Mock;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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
}
