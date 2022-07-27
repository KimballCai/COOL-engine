package com.nus.cool.core.io.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.readstore.CubeMetaRS;
import com.nus.cool.core.io.writestore.MetaChunkWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.parser.CsvTupleParser;
import com.nus.cool.core.util.parser.TupleParser;
import com.nus.cool.core.util.reader.LineTupleReader;
import com.nus.cool.core.util.reader.TupleReader;

public class CubeMetaTest {
  static final Logger logger = LoggerFactory.getLogger(CubeMetaTest.class);
  private TableSchema schema;
  private TupleReader reader;
  private TupleParser parser;
  private MetaChunkWS metaws;

  private String[] expected = {
    "{\"charset\":\"UTF-8\",\"type\":\"UserKey\",\"values\":[\"P-0\",\"P-1\",\"P-10\",\"P-11\",\"P-12\",\"P-2\",\"P-3\",\"P-4\",\"P-5\",\"P-6\",\"P-7\",\"P-8\",\"P-9\"]}",
    "{\"type\":\"Metric\",\"min\":1954,\"max\":2000}",
    "{\"charset\":\"UTF-8\",\"type\":\"Action\",\"values\":[\"diagnose\",\"labtest\",\"prescribe\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Disease-A\",\"Disease-B\",\"Disease-C\",\"None\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Medicine-A\",\"Medicine-B\",\"Medicine-C\",\"None\"]}",
    "{\"charset\":\"UTF-8\",\"type\":\"Segment\",\"values\":[\"Labtest-A\",\"Labtest-B\",\"Labtest-C\",\"None\"]}",
    "{\"type\":\"Metric\",\"min\":0,\"max\":76}",
    "{\"type\":\"ActionTime\",\"min\":15340,\"max\":15696}"
  };

  @BeforeTest
  public void setup() {
    logger.info("Start UnitTest " + CubeMetaTest.class.getSimpleName());
    String sourcePath = Paths.get(System.getProperty("user.dir"),
      "src",
      "test",
      "java",
      "com",
      "nus",
      "cool",
      "core",
      "resources").toString();
    String schemaPath = Paths.get(sourcePath, "health", "table.yaml").toString();
    try {
      this.schema = TableSchema.read(new File(schemaPath));
    } catch (IOException e) {
      System.err.println("cannot read test data schema file");
      e.printStackTrace();
    }
    
    String dataPath = Paths.get(sourcePath, "health", "table.csv").toString();
    try {
      this.reader = new LineTupleReader(new File(dataPath));
    } catch (IOException e) {
      System.err.println("cannot read test data csv file");
      e.printStackTrace();
    }
    this.parser = new CsvTupleParser();

    this.metaws = MetaChunkWS.newMetaChunkWS(this.schema, 0);
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", CubeMetaTest.class.getSimpleName()));
  }

  @Test
  public void CubeMetaUnbitTest() throws IOException {
    while (reader.hasNext()) {
      metaws.put(parser.parse(reader.next()));
    }
    
    DataOutputBuffer out = new DataOutputBuffer();
    metaws.complete();
    int written = metaws.writeCubeMeta(out);
    out.close();

    ByteBuffer buffer = ByteBuffer.wrap(out.getData())
      .order(ByteOrder.nativeOrder());
    buffer.limit(written);

    CubeMetaRS cubemeta = new CubeMetaRS(this.schema);
    cubemeta.readFrom(buffer);
    int idx = 0;
    for (FieldSchema field : schema.getFields()) {
      String fieldmeta = cubemeta.getFieldMeta(field.getName());
      Assert.assertEquals(fieldmeta, expected[idx++]);
    }
  }
}