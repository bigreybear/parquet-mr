package org.apache.tsfile.bmtool;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.parquet.bmtool.DataSets;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ConditionGenerator implements Serializable {

  private static final long serialVersionUID = 3267199406246928943L;

  // how many conditions per type
  public static int CONDITION_CARD = 10;
  // how many points per range
  public static int RANGE_SPAN = 30;

  public abstract class CompCondition implements Serializable {
    public String series;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CompCondition a = (CompCondition) o;
      return series.equals(a.series);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(series);
    }
  }

  public class TimeRange extends CompCondition implements Serializable{
    public long t1, t2;
    public TimeRange(String s, long t1, long t2) {
      this.series = s;
      this.t1 = t1;
      this.t2 = t2;
    }
  }

  public class DoubleRange extends CompCondition implements Serializable {
    public double v1, v2;
    public DoubleRange(String s, double d1, double d2) {
      series = s;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);;
    }
  }

  public class MixedRange extends CompCondition implements Serializable {
    public double v1, v2;
    public long t1, t2;
    public MixedRange(String s, long t1, long t2, double d1, double d2) {
      series = s;
      this.t1 = t1;
      this.t2 = t2;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);
    }
  }

  public class CrossRange extends CompCondition implements Serializable {
    public String cSeries; // c for crossed, inherited series to be filtered
    public double v1, v2;
    public CrossRange(String filtered, String crossSelected, double d1, double d2) {
      series = filtered; cSeries = crossSelected;
      v1 = Math.min(d1, d2);
      v2 = Math.max(d1, d2);
    }
  }

  public Set<String> singleSeries = new HashSet<>();
  public Set<String> alignedDevices = new HashSet<>();
  public Set<TimeRange> timeRanges = new HashSet<>();
  public Set<DoubleRange> doubleRanges = new HashSet<>();
  public Set<MixedRange> mixedRanges = new HashSet<>();
  public Set<CrossRange> crossRanges = new HashSet<>();

  public ConditionGenerator() {}

  private void sampleSeries(LargeVarCharVector idv, String sensor) {
    int total = idv.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.6 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idv.get(idx), StandardCharsets.UTF_8);
      singleSeries.add(dev + "." + sensor);
    }
  }

  private void sampleAligned(LargeVarCharVector idv) {
    int total = idv.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.2 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idv.get(idx), StandardCharsets.UTF_8);
      alignedDevices.add(dev);
    }
  }

  private void sampleTimeRanges(LargeVarCharVector idVector, BigIntVector tVector, String sensor) {
    int total = idVector.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.2 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idVector.get(idx), StandardCharsets.UTF_8);
      int steps = 1;
      int idxOffset = 0;
      while (steps < RANGE_SPAN + idxOffset) {
        String ndev = new String(idVector.get(idx + steps), StandardCharsets.UTF_8);
        if (!ndev.equals(dev)) {
          // if occurred last row of one device, examine next device as condition with offset
          if (steps == 1 + idxOffset) {
            dev = ndev;
            // now to measure from here, limit of steps expands with offset as well
            idxOffset = steps;
            steps ++;
            continue;
          }
          steps --;
          break;
        }
        steps++;
      }

      String series = dev + "." + sensor;
      timeRanges.add(
          new TimeRange(
              series,
              tVector.get(idx + idxOffset),
              tVector.get(idx + idxOffset + steps))
      );
    }
  }

  private void sampleDoubleRanges(LargeVarCharVector idVector, Float8Vector tVector, String sensor) {
    int total = idVector.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.1 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idVector.get(idx), StandardCharsets.UTF_8);
      int steps = 1;
      int idxOffset = 0;
      while (steps < RANGE_SPAN + idxOffset) {
        String ndev = new String(idVector.get(idx + steps), StandardCharsets.UTF_8);
        if (!ndev.equals(dev)) {
          // if occurred last row of one device, examine next device as condition with offset
          if (steps == 1) {
            dev = ndev;
            // now to measure from here, limit of steps expands with offset as well
            idxOffset = steps;
            steps ++;
            continue;
          }
          steps --;
          break;
        }
        steps++;
      }

      String series = dev + "." + sensor;

      if (tVector.isNull(idx+idxOffset)
          || tVector.isNull(idx+idxOffset+steps)
          || tVector.get(idx + idxOffset) == tVector.get(idx + idxOffset + steps)) {
        continue;
      }
      doubleRanges.add(
          new DoubleRange(
              series,
              tVector.get(idx + idxOffset),
              tVector.get(idx + idxOffset + steps))
      );
    }
  }

  private void sampleCrossRanges(LargeVarCharVector idVector,
                                 Float8Vector tVector,
                                 String filtered,
                                 String crossSelected) {
    int total = idVector.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.13 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idVector.get(idx), StandardCharsets.UTF_8);
      int steps = 1;
      int idxOffset = 0;
      while (steps < RANGE_SPAN + idxOffset) {
        String ndev = new String(idVector.get(idx + steps), StandardCharsets.UTF_8);
        if (!ndev.equals(dev)) {
          // if occurred last row of one device, examine next device as condition with offset
          if (steps == 1) {
            dev = ndev;
            // now to measure from here, limit of steps expands with offset as well
            idxOffset = steps;
            steps ++;
            continue;
          }
          steps --;
          break;
        }
        steps++;
      }

      String series = dev + "." + filtered;

      if (tVector.isNull(idx+idxOffset)
          || tVector.isNull(idx+idxOffset+steps)
          || tVector.get(idx + idxOffset) == tVector.get(idx + idxOffset + steps)) {
        continue;
      }
      crossRanges.add(
          new CrossRange(
              series,
              dev + "." + crossSelected,
              tVector.get(idx + idxOffset),
              tVector.get(idx + idxOffset + steps))
      );
    }
  }

  private void sampleMixedRanges(LargeVarCharVector idVector,
                                 BigIntVector bigIntVector,
                                 Float8Vector float8Vector,
                                 String sensor) {
    int total = idVector.getValueCount();
    int span = total/CONDITION_CARD;
    int shift = (int) (0.7 * span);

    String dev;
    for (int idx = shift; idx < total; idx += span) {
      dev = new String(idVector.get(idx), StandardCharsets.UTF_8);
      int steps = 1;
      int idxOffset = 0;
      while (steps < RANGE_SPAN + idxOffset) {
        String ndev = new String(idVector.get(idx + steps), StandardCharsets.UTF_8);
        if (!ndev.equals(dev)) {
          // if occurred last row of one device, examine next device as condition with offset
          if (steps == 1) {
            dev = ndev;
            // now to measure from here, limit of steps expands with offset as well
            idxOffset = steps;
            steps ++;
            continue;
          }
          steps --;
          break;
        }
        steps++;
      }

      String series = dev + "." + sensor;
      mixedRanges.add(
          new MixedRange(
              series,
              bigIntVector.get(idx + idxOffset),
              bigIntVector.get(idx + idxOffset + steps),
              float8Vector.get(idx + idxOffset),
              float8Vector.get(idx + idxOffset + steps))
      );
    }
  }

  public void sampleAll(LargeVarCharVector idVector,
                        BigIntVector bigIntVector,
                        Float8Vector float8Vector,
                        String sensor,
                        String crossSensor) {
    sampleAligned(idVector);
    sampleSeries(idVector, sensor);
    sampleTimeRanges(idVector, bigIntVector, sensor);
    sampleDoubleRanges(idVector, float8Vector, sensor);
    sampleMixedRanges(idVector, bigIntVector, float8Vector, sensor);
    sampleCrossRanges(idVector, float8Vector, sensor, crossSensor);
  }

  public static void serialize(ConditionGenerator cg, String path) throws IOException {
    FileOutputStream fos = new FileOutputStream(path);
    ObjectOutputStream oos = new ObjectOutputStream(fos);

    oos.writeObject(cg);
    oos.close();
  }

  public static ConditionGenerator deserialize(String path) throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))){
      return (ConditionGenerator) ois.readObject();
    }
  }


  public static ConditionGenerator getConditionsByDataSets(DataSets dataset) throws IOException, ClassNotFoundException {
    return ConditionGenerator.deserialize(dataset.getConditionBinPath());
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    // generateTDriveConditions();
    // generateREDDConditions();

    // generateGeoLifeConditions();
    // generateTSBSConditions();
    ConditionGenerator cg = ConditionGenerator.deserialize(DataSets.TSBS.getConditionBinPath());
    System.out.println("sc?");
  }

}
