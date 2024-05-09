package org.apache.parquet.bmtool;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.tsfile.bmtool.ConditionGenerator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BMReader {

  public static String DST_DIR = "F:\\0006DataSets\\EXP5-3\\";
  public static String DATE_STR = java.time.format.DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .format(java.time.LocalDateTime.now());

  public static String NAME_COMMENT = "_NO_SPEC_";

  public static File paqFile;
  public static File logFile;

  public static BufferedWriter logger;

  public static String getDeviceFromNodes(String[] array) {
    if (array == null || array.length == 0 ) {
      return "";
    }
    if (array.length == 2) {
      return array[0];
    }
    StringBuilder sb = new StringBuilder(array[0]);
    for (int i = 1; i < array.length - 1; i++) {
      sb.append(".");
      sb.append(array[i]);
    }
    return sb.toString();
  }

  // build expression objects for TsFile
  public static class QueryBuilder {
    public static List<FilterCompat.Filter> singleSeries(ConditionGenerator cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (String series : cg.singleSeries) {
        String[] nodes = series.split("\\.");
        String device = getDeviceFromNodes(nodes);
        expressions.add(DATA_SET.getDeviceEquation(device));
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> alignedDevices(ConditionGenerator cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (String device : cg.alignedDevices) {
        expressions.add(DATA_SET.getDeviceEquation(device));
      }
      return expressions;
    }

    // device equation filter is necessary for all expressions
    public static List<FilterCompat.Filter> timeExpression(ConditionGenerator cg) {
      List<FilterCompat.Filter> deviceEqs = singleSeries(cg);
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGenerator.TimeRange tr : cg.timeRanges) {
        String[] nodes = tr.series.split("\\.");
        FilterCompat.Filter tFilter =
            FilterCompat.get(
                FilterApi.and(
                 DATA_SET.getDevicePredicate(getDeviceFromNodes(nodes)),
                  FilterApi.and(
                      FilterApi.gtEq(FilterApi.longColumn("timestamp"), tr.t1),
                      FilterApi.ltEq(FilterApi.longColumn("timestamp"), tr.t2)
                  )
                )
            );
        expressions.add(tFilter);
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> valueExpression(ConditionGenerator cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGenerator.DoubleRange dr: cg.doubleRanges) {
        String[] nodes = dr.series.split("\\.");
        String sensorCol = nodes[nodes.length - 1];
        FilterCompat.Filter vFilter =
            FilterCompat.get(
                FilterApi.and(
                    DATA_SET.getDevicePredicate(getDeviceFromNodes(nodes)),
                    FilterApi.and(
                        FilterApi.gtEq(FilterApi.doubleColumn(sensorCol), dr.v1),
                        FilterApi.ltEq(FilterApi.doubleColumn(sensorCol), dr.v2)
                    )
                )
            );
        expressions.add(vFilter);
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> crossExpression(ConditionGenerator cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGenerator.CrossRange cr: cg.crossRanges) {
        String[] nodes = cr.series.split("\\.");
        String sensorCol = nodes[nodes.length - 1];
        FilterCompat.Filter vFilter =
            FilterCompat.get(
                FilterApi.and(
                    DATA_SET.getDevicePredicate(getDeviceFromNodes(nodes)),
                    FilterApi.and(
                        FilterApi.gtEq(FilterApi.doubleColumn(sensorCol), cr.v1),
                        FilterApi.ltEq(FilterApi.doubleColumn(sensorCol), cr.v2)
                    )
                )
            );
        expressions.add(vFilter);
      }
      return expressions;
    }

    public static List<FilterCompat.Filter> mixedExpression(ConditionGenerator cg) {
      List<FilterCompat.Filter> expressions = new ArrayList<>();
      for (ConditionGenerator.MixedRange mr : cg.mixedRanges) {
        String[] nodes = mr.series.split("\\.");
        String sensorCol = nodes[nodes.length - 1];
        FilterPredicate tFilter =
                FilterApi.and(
                    FilterApi.gtEq(FilterApi.longColumn("timestamp"), mr.t1),
                    FilterApi.ltEq(FilterApi.longColumn("timestamp"), mr.t2)
                );
        FilterPredicate vFilter =
                FilterApi.and(
                    FilterApi.gtEq(FilterApi.doubleColumn(sensorCol), mr.v1),
                    FilterApi.ltEq(FilterApi.doubleColumn(sensorCol), mr.v2)
                );
        FilterPredicate dvFilter = FilterApi.and(
            vFilter,
            DATA_SET.getDevicePredicate(getDeviceFromNodes(nodes))
        );
        expressions.add(FilterCompat.get(FilterApi.and(tFilter, dvFilter)));
      }
      return expressions;
    }
  }
  // count tuples
  public static ExecuteResult execute(ParquetReader.Builder builder, List<FilterCompat.Filter> exps) throws IOException {
    int cnt = 0;
    long time = System.nanoTime();
    ParquetReader<Group> reader;
    Group record;
    Stopwatch.zero();
    for (FilterCompat.Filter e : exps) {
      int s20 = 20;
      builder.withFilter(e);
      reader = builder.build();
      Stopwatch.start();
      while ((record = reader.read()) != null && s20 > 0) {
        cnt++;
        s20--;
      }
      Stopwatch.stop();
      reader.close();
    }
    time = System.nanoTime() - time;
    System.out.println("Result count:" + cnt);
    return new ExecuteResult(Stopwatch.report()/exps.size(), cnt);
  }


  // prepare reader here
  public static void bmQuery(ParquetReader.Builder readerBuilder, ConditionGenerator cg, int queryType, int dataSets) throws IOException {
    PlainParquetConfiguration configuration = new PlainParquetConfiguration();
    List<FilterCompat.Filter> queryExpressions;

    switch (QueryType.values()[queryType]) {
      case SingleRaw:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getSingleSchema(DATA_SET.comparingColumn).toString()
        );
        queryExpressions = QueryBuilder.singleSeries(cg);
        break;
      case AlignedRaw:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getAlignedSchema().toString()
        );
        queryExpressions = QueryBuilder.alignedDevices(cg);
        break;
      case TimeFilter:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getSingleSchema(DATA_SET.comparingColumn).toString()
        );
        queryExpressions = QueryBuilder.timeExpression(cg);
        break;
      case valueFilter:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getSingleSchema(DATA_SET.comparingColumn).toString()
        );
        queryExpressions = QueryBuilder.valueExpression(cg);
        break;
      case MixedFilter:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getSingleSchema(DATA_SET.comparingColumn).toString()
        );
        queryExpressions = QueryBuilder.mixedExpression(cg);
        break;
      case CrossFilter:
        configuration.set(
            ReadSupport.PARQUET_READ_SCHEMA,
            DATA_SET.getCrossSchema(DATA_SET.comparingColumn, DATA_SET.getCorssColumn()).toString()
        );
        queryExpressions = QueryBuilder.crossExpression(cg);
        break;
      default:
        queryExpressions = null;
    }
    readerBuilder.withConf(configuration);
    ExecuteResult res = execute(readerBuilder, queryExpressions);
    formattedLog(DataSets.values()[dataSets].toString(), queryType, res.latency, res.rowCnt);
  }

  private static class ExecuteResult {
    long latency;
    int rowCnt; // to check answer
    public ExecuteResult (long l, int r) {
      latency = l; rowCnt = r;
    }
  }

  public static void init() throws IOException {
    paqFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void formattedLog(String ds, int queryType, long latency, int rowCnt) throws IOException {
    logger.write(String.format("%s\t%s\t%s\t%d\t%d\t%s",
        ds, QueryType.values()[queryType], DATE_STR, latency/1000000, rowCnt, paqFile.getName()));
    logger.newLine();
  }

  public static DataSets DATA_SET = DataSets.TSBS; // datasets using distinct schemas
  public static String LOG_PATH = DST_DIR + "PAQ_FILE_Query_Results.log";
  public static String FILE_PATH = DST_DIR + DATA_SET.getTargetFile();

  // to fix too late read
  public static long READ_MAX = 1000;

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    int queryType = 0;
    FILE_PATH = DST_DIR + "PAQ_FILE__TSBS-5.3.2-length-1_.parquet";
    DATA_SET = DataSets.TSBS;
    if (args.length >= 3) {
      FILE_PATH = DST_DIR + args[0];
      DATA_SET = DataSets.valueOf(args[1]);
      queryType = Integer.parseInt(args[2]);
    } else {
      // System.out.println("Not enough arguments.");
      // return;
    }

    init();
    ConditionGenerator cg = ConditionGenerator.getConditionsByDataSets(DATA_SET);
    ParquetReader.Builder builder = ParquetReader.builder(new GroupReadSupport(), new Path(FILE_PATH));
    bmQuery(builder, cg, queryType, DATA_SET.ordinal());
    logger.close();

    // PlainParquetConfiguration configuration = new PlainParquetConfiguration();
    // configuration.set(ReadSupport.PARQUET_READ_SCHEMA, DataSets.TDrive.getSingleSchema("lat").toString());
    // ParquetReader<Group> reader = ParquetReader
    //     .builder(new GroupReadSupport(), new Path(FILE_PATH))
    //     .withConf(configuration)
    //     .build();
    // Group record;
    // while ((record = reader.read()) != null) {
    //   MessageType schema = (MessageType) record.getType();
    //   for (Type field : schema.getFields()) {
    //     int valueCount = record.getFieldRepetitionCount(field.getName());
    //     if (valueCount > 0) {
    //       // System.out.println(field.getName() + ": " + record.getValueToString(0, field.getName()));
    //     }
    //   }
    // }
    // reader.close();


    // ParquetReader<Group> reader =
    // TsFileReader reader = new TsFileReader(new TsFileSequenceReader(FILE_PATH));
    // ConditionGenerator conditions = ConditionGenerator.getConditionsByDataSets(DataSets.TDrive);
    // bmQuery(reader, conditions, 4, 0);
    // logger.close();
  }

  public enum QueryType {
    SingleRaw,
    AlignedRaw,
    TimeFilter,
    valueFilter,
    MixedFilter,
    CrossFilter
  }
}
