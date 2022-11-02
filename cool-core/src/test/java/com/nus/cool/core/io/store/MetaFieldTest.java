package com.nus.cool.core.io.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.compression.OutputCompressor;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.readstore.MetaHashFieldRS;
import com.nus.cool.core.io.readstore.MetaRangeFieldRS;
import com.nus.cool.core.io.writestore.MetaFieldWS;
import com.nus.cool.core.io.writestore.MetaHashFieldWS;
import com.nus.cool.core.io.writestore.MetaRangeFieldWS;
import com.nus.cool.core.schema.FieldSchema;
import com.nus.cool.core.schema.FieldType;
import com.nus.cool.core.schema.TableSchema;
import com.nus.cool.core.util.converter.DayIntConverter;

/** In this unit, we test MetaHashField and MetaRangeField independently */
public class MetaFieldTest {

  static final Logger logger = LoggerFactory.getLogger(MetaFieldTest.class);

  private Charset charset;
  private OutputCompressor compressor;
  // private TestTable table;

  @BeforeTest
  public void setUp() {
    logger.info("Start UnitTest " + MetaFieldTest.class.getSimpleName());
    this.charset = Charset.defaultCharset();
    this.compressor = new OutputCompressor();
    // // temporary test dataset
    // String sourcePath = Paths.get(System.getProperty("user.dir"),
    // "src",
    // "test",
    // "java",
    // "com",
    // "nus",
    // "cool",
    // "core",
    // "resources").toString();
    // String filepath = Paths.get(sourcePath, "fieldtest", "table.csv").toString();
    // this.table = TestTable.readFromCSV(filepath);
  }

  @AfterTest
  public void tearDown() {
    logger.info(String.format("Tear down UnitTest %s\n", MetaFieldTest.class.getSimpleName()));
  }

  @Test(dataProvider = "MetaFieldDP", enabled = false)
  public void MetaFieldUnitTest(String dataDirPath) throws IOException {
    TestTable table = utils.loadTable(dataDirPath);
    TableSchema schema = utils.loadSchema(dataDirPath);
    for (FieldSchema fieldSchema : schema.getFields()) {
      if (fieldSchema.getFieldType() == FieldType.UserKey) continue;

      if (FieldType.isHashType(fieldSchema.getFieldType())) {
        MetaHashFieldTest(table, fieldSchema.getName(), fieldSchema.getFieldType());
      } else {
        MetaRangeFieldTest(table, fieldSchema.getName(), fieldSchema.getFieldType());
      }
    }
  }

  @DataProvider(name = "MetaFieldDP")
  public Object[][] MetaFieldDataProvider() {
    String sourcePath =
        Paths.get(
                System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources")
            .toString();
    String HealthPath = Paths.get(sourcePath, "health").toString();
    String TPCHPath = Paths.get(sourcePath, "olap-tpch").toString();
    String SogamoPath = Paths.get(sourcePath, "sogamo").toString();
    return new Object[][] {
      {HealthPath}, {TPCHPath}, {SogamoPath},
    };
  }

  /**
   * For test the logic of Test unit In product env, enanble = false
   *
   * @param table
   * @param fieldName
   * @param type
   * @throws IOException
   */
  @Test(dataProvider = "MetaHashFieldDP", enabled = false)
  public void MetaHashFieldUnitTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    MetaHashFieldTest(table, fieldName, type);
  }

  @DataProvider(name = "MetaHashFieldDP")
  public Object[][] MetaHashFieldDataProvider() {
    String sourcePath =
        Paths.get(
                System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources")
            .toString();
    String HealthPath = Paths.get(sourcePath, "health").toString();
    System.out.println(HealthPath);
    TestTable table = utils.loadTable(HealthPath);
    System.out.println(table.toString());
    table.ShowTable();
    return new Object[][] {
      {table, "event", FieldType.Action},
      {table, "attr1", FieldType.Segment},
      {table, "attr2", FieldType.Segment},
      {table, "attr3", FieldType.Segment},
    };
  }

  @Test(dataProvider = "MetaRangeFieldDP", enabled = false)
  public void MetaRangeFieldUnitTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    MetaRangeFieldTest(table, fieldName, type);
  }

  @DataProvider(name = "MetaRangeFieldDP")
  public Object[][] MetaRangeFieldDataProvider() {
    String sourcePath =
        Paths.get(
                System.getProperty("user.dir"),
                "src",
                "test",
                "java",
                "com",
                "nus",
                "cool",
                "core",
                "resources")
            .toString();
    String HealthPath = Paths.get(sourcePath, "health").toString();
    System.out.println(HealthPath);
    TestTable table = utils.loadTable(HealthPath);
    System.out.println(table.toString());
    table.ShowTable();
    return new Object[][] {
      {"birthYear", FieldType.Metric},
      {"attr4", FieldType.Metric},
      {"time", FieldType.ActionTime}
    };
  }

  public void MetaHashFieldTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    System.out.println(fieldName + type.toString());
    int fieldIdx = table.getField2Ids().get(fieldName);
    System.out.println(fieldIdx);
    MetaFieldWS mws = new MetaHashFieldWS(type, this.charset, this.compressor);

    // ground-truth value
    // value : gloablId
    Map<String, Integer> res = new HashMap<>();
    int gid = 0;
    for (int idx = 0; idx < table.getRowCounts(); idx++) {
      String[] tuple = table.getTuple(idx);
      mws.put(tuple, fieldIdx);
      if (!res.containsKey(tuple[fieldIdx])) {
        res.put(tuple[fieldIdx], gid++);
      }
    }

    // write
    DataOutputBuffer dob = new DataOutputBuffer();
    mws.writeTo(dob);
    // set byteBuffer
    ByteBuffer bf = ByteBuffer.wrap(dob.getData());
    bf.order(ByteOrder.nativeOrder());

    // read
    MetaFieldRS mrs = new MetaHashFieldRS(this.charset);
    mrs.readFromWithFieldType(bf, type);

    Assert.assertEquals(mrs.count(), res.size());
    Assert.assertEquals(mrs.getMinValue(), 0);

    for (Map.Entry<String, Integer> entry : res.entrySet()) {
      int actual = mrs.find(entry.getKey());
      int expect = entry.getValue();
      Assert.assertEquals(actual, expect);
    }
  }

  public void MetaRangeFieldTest(TestTable table, String fieldName, FieldType type)
      throws IOException {
    int fieldIdx = table.getField2Ids().get(fieldName);
    MetaFieldWS mws = new MetaRangeFieldWS(type);
    DayIntConverter converter = DayIntConverter.getInstance();
    int max = Integer.MIN_VALUE;
    int min = Integer.MAX_VALUE;

    for (int idx = 0; idx < table.getRowCounts(); idx++) {
      String[] tuple = table.getTuple(idx);
      mws.put(tuple, fieldIdx);
      int v = 0;
      if (type == FieldType.ActionTime) {
        v = converter.toInt(tuple[fieldIdx]);
      } else {
        v = Integer.parseInt(tuple[fieldIdx]);
      }
      min = Math.min(min, v);
      max = Math.max(max, v);
    }

    // write
    DataOutputBuffer dob = new DataOutputBuffer();
    mws.writeTo(dob);

    ByteBuffer bf = ByteBuffer.wrap(dob.getData());
    bf.order(ByteOrder.nativeOrder());

    // read
    MetaFieldRS mrs = new MetaRangeFieldRS();
    mrs.readFromWithFieldType(bf, type);

    Assert.assertEquals(mrs.getMaxValue(), max);
    Assert.assertEquals(mrs.getMinValue(), min);
  }
}
