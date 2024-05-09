package org.apache.parquet.bmtool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.Float8Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;

public class BMWriter53 {

  public static class DataSetsProfile {
    public static int deviceNum = 0;
    public static int seriesNum = 0;
    public static long ptsNum = 0;
  }

  public static boolean TO_PROFILE = true;

  public static String DST_DIR = "F:\\0006DataSets\\EXP5-3\\";
  public static String DATE_STR = java.time.format.DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .format(java.time.LocalDateTime.now());

  public static File parqFile;
  public static File logFile;

  // use group for rough evaluation
  public static ParquetWriter<Group> writer;
  public static BufferedWriter logger;

  public static int BATCH = 1024;
  public static Configuration conf = new Configuration();
  public static ParquetConfiguration plainParquetConf = new PlainParquetConfiguration();
  public static MessageType schema;


  public static void init() throws IOException {
    parqFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);

    // Path root = new Path("target/tests/TestParquetWriter/");
    schema = CUR_DATA.schema;
    // GroupWriteSupport.setSchema(schema, plainParquetConf);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void closeAndSummary() throws IOException {
    writer.close();
    logger.write(String.format("Load: %s, Comment:%s, File:%s", CUR_DATA, NAME_COMMENT, parqFile.getName()));
    logger.newLine();
    logger.write(String.format("Pts: %d, Series: %d, Devs: %d\n",
        DataSetsProfile.ptsNum, DataSetsProfile.seriesNum, DataSetsProfile.deviceNum));
    writer.report(logger);
    logger.write("==========================================");
    logger.newLine();
  }


  public static void bmTSBS() throws IOException {
    writer = ExampleParquetWriter.builder(new LocalOutputFile(parqFile.toPath()))
        .withConf(plainParquetConf).withType(schema)
        // .withDictionaryEncoding("lat", encodeSeriesColumn)
        // .withDictionaryEncoding("lon", encodeSeriesColumn)
        // .withDictionaryEncoding("ele", encodeSeriesColumn)
        // .withDictionaryEncoding("vel", encodeSeriesColumn)
        .withCompressionCodec(compressionCodecName)
        .build();

    TSBSLoader loader = TSBSLoader.deserialize("F:\\0006DataSets\\Arrows-5-3\\", E53_NAME);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    String preDev = new String(loader.idVector.get(0), StandardCharsets.UTF_8);
    String curDev = null;
    String[] nodes = preDev.split("\\.");
    String fleet = nodes[0], name = nodes[1], driver = nodes[2];

    List<Group> batchGroups = new ArrayList<>(BATCH);
    Group g;
    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L, curTS; // filter out-of-order data

    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 4;
        }

        for (Group ig : batchGroups) {
          writer.write(ig);
        }
        batchGroups.clear();

        preDev = curDev;
        nodes = curDev.split("\\.");
        fleet = nodes[0];
        name = nodes[1];
        driver = nodes[2];

        lastTS = -1;
      }


      curTS = loader.timestampVector.get(cnt);
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }

      g = f.newGroup().append("fleet", fleet)
          .append("name", name).append("driver", driver)
          .append("timestamp", loader.timestampVector.get(cnt));

      g = appendIfNotNull(g, "lat", loader.latVec, cnt);
      g = appendIfNotNull(g, "lon", loader.lonVec, cnt);
      g = appendIfNotNull(g, "ele", loader.eleVec, cnt);
      g = appendIfNotNull(g, "vel", loader.velVec, cnt);

      batchGroups.add(g);
      // batchGroups.add(f.newGroup()
      //     .append("fleet", fleet)
      //     .append("name", name).append("driver", driver)
      //     .append("timestamp", loader.timestampVector.get(cnt));

      lastTS = loader.timestampVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 4;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }
  }

  public static Group appendIfNotNull(Group group, String fn, Float8Vector vec, int idx) {
    if (!vec.isNull(idx)) {
      group.append(fn, vec.get(idx));
    }
    return group;
  }

  public static void bmREDD() throws IOException {
    writer = ExampleParquetWriter.builder(new LocalOutputFile(parqFile.toPath()))
        .withConf(plainParquetConf).withType(schema)
        // .withDictionaryEncoding("elec", encodeSeriesColumn)
        .withCompressionCodec(compressionCodecName)
        .build();

    REDDLoader loader = REDDLoader.deserialize(CUR_DATA.getArrowFile());
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    String preDev = new String(loader.idVector.get(0), StandardCharsets.UTF_8);
    String curDev = null;
    String[] nodes = preDev.split("\\.");
    String building = nodes[0], meter = nodes[1];

    List<Group> batchGroups = new ArrayList<>(BATCH);
    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L, curTS; // filter out-of-order data

    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 1;
        }

        for (Group g : batchGroups) {
          writer.write(g);
        }
        batchGroups.clear();

        preDev = curDev;
        nodes = curDev.split("\\.");
        building = nodes[0];
        meter = nodes[1];

        lastTS = -1;
      }


      curTS = loader.timestampVector.get(cnt);
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }


      batchGroups.add(f.newGroup()
          .append("building", building)
          .append("meter", meter)
          .append("timestamp", loader.timestampVector.get(cnt))
          .append("elec", loader.elecVector.get(cnt)));


      lastTS = loader.timestampVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 1;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }
  }

  public static void bmGeoLife(long limit) throws IOException {
    // plainParquetConf.set("parquet.example.schema", schema);
    writer = ExampleParquetWriter.builder(new LocalOutputFile(parqFile.toPath()))
        .withConf(plainParquetConf).withType(schema)
        // .withDictionaryEncoding("lon", encodeSeriesColumn)
        // .withDictionaryEncoding("lat", encodeSeriesColumn)
        // .withDictionaryEncoding("alt", encodeSeriesColumn)
        .withCompressionCodec(compressionCodecName)
        .build();

    GeoLifeLoader loader = GeoLifeLoader.deserialize(CUR_DATA.getArrowFile());
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    String preDev = new String(loader.idVector.get(0), StandardCharsets.UTF_8);
    String curDev = null;
    List<Group> batchGroups = new ArrayList<>(BATCH);
    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L, curTS; // filter out-of-order data

    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 3;
        }

        for (Group g : batchGroups) {
          writer.write(g);
        }
        batchGroups.clear();

        preDev = curDev;
        lastTS = -1;
      }


      curTS = loader.timestampVector.get(cnt);
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }

      batchGroups.add(f.newGroup()
          .append("deviceID", curDev)
          .append("timestamp", loader.timestampVector.get(cnt))
          .append("lon", loader.longitudeVector.get(cnt))
          .append("lat", loader.latitudeVector.get(cnt))
          .append("alt", loader.altitudeVector.get(cnt)));


      lastTS = loader.timestampVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 3;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }
  }

  public static void bmTDrive(long limit) throws IOException {
    // plainParquetConf.set("parquet.example.schema", schema);
    writer = ExampleParquetWriter.builder(new LocalOutputFile(parqFile.toPath()))
        .withConf(plainParquetConf).withType(schema)
        // .withDictionaryEncoding("lon", encodeSeriesColumn)
        // .withDictionaryEncoding("lat", encodeSeriesColumn)
        .withCompressionCodec(compressionCodecName)
        .build();

    TDriveLoader loader = TDriveLoader.deserialize(CUR_DATA.getArrowFile());
    // loader.load(Long.MAX_VALUE);
    // loader.load(limit);

    // GroupWriteSupport.setSchema(schema, (Configuration) plainParquetConf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    String preDev = new String(loader.idVector.get(0), StandardCharsets.UTF_8);
    String curDev = null;
    List<Group> batchGroups = new ArrayList<>(BATCH);
    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L, curTS; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), StandardCharsets.UTF_8);
      if (!preDev.equals(curDev)) {
        rowInTablet = 0;

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 2;
        }

        for (Group g : batchGroups) {
          writer.write(g);
        }
        batchGroups.clear();

        preDev = curDev;
        lastTS = -1;
      }


      curTS = loader.timestampVector.get(cnt);
      if (curTS <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }

      batchGroups.add(f.newGroup()
          .append("deviceID", curDev)
          .append("timestamp", loader.timestampVector.get(cnt))
          .append("lon", loader.longitudeVector.get(cnt))
          .append("lat", loader.latitudeVector.get(cnt)));


      lastTS = loader.timestampVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 2;
      }
    }

    for (int i = 0; i < batchGroups.size(); i++) {
      writer.write(batchGroups.get(i));
    }

  }

  public static void checker() throws IOException {
    ParquetReader<Group> reader = ParquetReader
        .builder(new GroupReadSupport(), new Path(String.valueOf(parqFile)))
        .withConf(plainParquetConf).build();
    for (int i = 0; i < 10; i++) {
      Group g = reader.read();
      System.out.println(String.format("%d, %f, %f, %s",
          g.getLong("timestamp", 0),
          g.getDouble("lat", 0),
          g.getDouble("lon", 0),
          g.getBinary("deviceID", 0).toStringUsingUTF8()));
    }
    reader.close();
  }

  private static void commentOnName(String comment) {
    if (comment != null) {
      NAME_COMMENT = "_" + comment + "_";
      FILE_PATH = DST_DIR + "PAQ_FILE_" + NAME_COMMENT + ".parquet";
      // LOG_PATH = DST_DIR + "PAQ_FILE" + DATE_STR + NAME_COMMENT + ".log";
    }
  }

  public static String FILE_PATH = DST_DIR + "PAQ_FILE_" + DATE_STR + ".parquet";
  public static String LOG_PATH = DST_DIR + "PAQ_FILE_Write_Results.log";

  public static CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
  public static String NAME_COMMENT = "_" + compressionCodecName + "_";
  public static DataSets CUR_DATA = DataSets.TSBS;
  public static String E53_NAME = "TSBS-5.3.2-scale-4";
  public static void main(String[] args) throws IOException {
    // assign default
    compressionCodecName = CompressionCodecName.SNAPPY;

    // assign from args, args: [dataset]
    if (args.length >= 1) {
      CUR_DATA = DataSets.valueOf(args[0]);
    }

    commentOnName(E53_NAME);
    init();
    long record = System.currentTimeMillis();
    // write as needed
    switch (CUR_DATA) {
      case TDrive:
        bmTDrive(Long.MAX_VALUE);
        // bmTDrive(200);
        break;
      case GeoLife:
        bmGeoLife(Long.MAX_VALUE);
        break;
      case REDD:
        bmREDD();
        break;
      case TSBS:
        bmTSBS();
        break;
      default:
    }
    record = System.currentTimeMillis() - record;
    System.out.println(String.format("Load and Write for %d ms.", record));
    closeAndSummary();

    // checker();
    logger.close();
  }
}
