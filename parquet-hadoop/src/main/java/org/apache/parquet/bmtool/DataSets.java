package org.apache.parquet.bmtool;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public enum DataSets { // and its sensors
  TDrive(new ArrayList<>(Arrays.asList("lat", "lon")), createTDriveSchema(), "lat"),
  GeoLife(new ArrayList<>(Arrays.asList("lat", "lon", "alt")), createGeoLifeSchema(), "lat"),
  REDD(new ArrayList<>(Arrays.asList("elec")), createREDDSchema(), "elec"),
  TSBS(new ArrayList<>(Arrays.asList("lat", "lon", "ele", "vel")), createTSBSSchema(), "vel");

  public List<String> sensors;
  public MessageType schema;
  public String comparingColumn; // the sensor be compared and retrieved

  private static MessageType createTDriveSchema() {
    return parseMessageType("message TDrive { "
        + "required binary deviceID;"
        + "required int64 timestamp;"
        + "required double lon;"
        + "required double lat; "
        + "} ");
  }

  private static MessageType createGeoLifeSchema() {
    return parseMessageType("message GeoLife { "
        + "required binary deviceID;"
        + "required int64 timestamp;"
        + "required double lon;"
        + "required double lat;"
        + "required double alt;"
        + "} ");
  }

  // Note(zx) multiple-part device id
  private static MessageType createREDDSchema() {
    return parseMessageType("message REDD { "
        + "required binary building;"
        + "required binary meter;"
        + "required int64 timestamp;"
        + "required double elec;"
        + "} ");
  }

  private static MessageType createTSBSSchema() {
    return parseMessageType("message TSBS { "
        + "required binary name;"
        + "required binary fleet;"
        + "required binary driver;"
        + "required int64 timestamp;"
        + "optional double lat;"
        + "optional double lon;"
        + "optional double ele;"
        + "optional double vel;"
        + "} ");
  }

  DataSets(List<String> ls, MessageType mt, String s) {
    sensors = ls;
    schema = mt;
    comparingColumn = s;
  }

  public MessageType getAlignedSchema() {
    switch (this) {
      case TDrive:
        return parseMessageType("message TDrive { "
            + "required binary deviceID;"
            + "required int64 timestamp;"
            + "required double lon;"
            + "required double lat; "
            + "} ");
      case GeoLife:
        return parseMessageType("message GeoLife { "
            + "required binary deviceID;"
            + "required int64 timestamp;"
            + "required double lon;"
            + "required double lat;"
            + "required double alt;"
            + "} ");
      case REDD:
        return parseMessageType("message REDD { "
            + "required binary building;"
            + "required binary meter;"
            + "required int64 timestamp;"
            + "required double elec;"
            + "} ");
      case TSBS:
        return parseMessageType("message TSBS { "
            + "required binary name;"
            + "required binary fleet;"
            + "required binary driver;"
            + "required int64 timestamp;"
            + "optional double lat;"
            + "optional double lon;"
            + "optional double ele;"
            + "optional double vel;"
            + "} ");
      default:
        return null;
    }
  }

  /**
   * @param sensor must be column with double type!
   * @return
   */
  public MessageType getSingleSchema(String sensor) {
    switch (this) {
      case GeoLife:
      case TDrive:
      case REDD:
        return getAlignedSchema();
      case TSBS:
        return parseMessageType("message TSBS { "
            + "required binary name;"
            + "required binary fleet;"
            + "required binary driver;"
            + "required int64 timestamp;"
            + "optional double " + comparingColumn + ";"
            + "} ");
      default:
        return null;
    }
  }

  public MessageType getCrossSchema(String sensor, String sensor2) {
    switch (this) {
      case GeoLife:
      case TDrive:
        return parseMessageType("message TDrive { "
            + "required binary deviceID;"
            + "required int64 timestamp;"
            + "required double " + sensor + "; "
            + "required double " + sensor2 + "; "
            + "} ");
      case REDD:
        return getAlignedSchema();
      case TSBS:
        return parseMessageType("message TSBS { "
            + "required binary name;"
            + "required binary fleet;"
            + "required binary driver;"
            + "required int64 timestamp;"
            + "optional double " + sensor + ";"
            + "optional double " + sensor2 + ";"
            + "} ");
      default:
        return null;
    }
  }

  // just wrap predicate below
  public FilterCompat.Filter getDeviceEquation(String device) {
    switch (this) {
      case TSBS:
      case REDD:
      case GeoLife:
      case TDrive:
        return FilterCompat.get(this.getDevicePredicate(device));
      default:
        return null;
    }
  }

  // just wrap predicate below
  public String getCorssColumn() {
    switch (this) {
      case TSBS:
        return "lat";
      case REDD:
        return "elec";
      case GeoLife:
      case TDrive:
        return "lon";
      default:
        return null;
    }
  }

  public FilterPredicate getDevicePredicate(String device) {
    String[] deviceElems = device.split("\\.");
    switch (this) {
      case GeoLife:
      case TDrive:
        Binary targetDevice = new Binary.FromStringBinary(deviceElems[0]);
        return FilterApi.eq(FilterApi.binaryColumn("deviceID"), targetDevice);
      case REDD:
        Binary building = new Binary.FromStringBinary(deviceElems[0]);
        Binary meter = new Binary.FromStringBinary(deviceElems[1]);
        return FilterApi.and(
            FilterApi.eq(FilterApi.binaryColumn("building"), building),
            FilterApi.eq(FilterApi.binaryColumn("meter"), meter)
        );
      case TSBS:
        Binary fleet = new Binary.FromStringBinary(deviceElems[0]);
        Binary name = new Binary.FromStringBinary(deviceElems[1]);
        Binary driver = new Binary.FromStringBinary(deviceElems[2]);
        return FilterApi.and(
            FilterApi.eq(FilterApi.binaryColumn("fleet"), fleet),
            FilterApi.and(
                FilterApi.eq(FilterApi.binaryColumn("name"), name),
                FilterApi.eq(FilterApi.binaryColumn("driver"), driver)
            )
        );
      default:
        return null;
    }
  }

  public String getConditionBinPath() {
    switch (this) {
      case TDrive:
        return "F:\\0006DataSets\\Conditions\\TDrive-lat-10.bin";
      case GeoLife:
        return "F:\\0006DataSets\\Conditions\\GeoLife-lat-10.bin";
      case REDD:
        return "F:\\0006DataSets\\Conditions\\REDD-elec-10.bin";
      case TSBS:
        return "F:\\0006DataSets\\Conditions\\TSBS-vel-10.bin";
      default:
    }
    return null;
  }

  public String getTargetFile() {
    switch (this) {
      case GeoLife:
        return "PAQ_FILE_GeoLife_20240315143122_SNAPPY_.parquet";
      case TDrive:
        return "PAQ_FILE_TDrive_20240313032447_UNCOMPRESSED_.parquet";
      case REDD:
        return "PAQ_FILE_REDD_20240313032958_UNCOMPRESSED_.parquet";
        // return "PAQ_FILE_REDD_20240312211151_UNCOMPRESSED_.parquet";
      case TSBS:
        return "PAQ_FILE_TSBS_20240313031919_UNCOMPRESSED_.parquet";
      default:
        return null;
    }
  }

  public String getArrowFile() {
    switch (this) {
      case TDrive:
        return "TDrive.bin";
      case GeoLife:
        return "GeoLife.bin";
      case REDD:
        return "REDD.bin";
      case TSBS:
        return "TSBS.bin";
      default:
    }
    return null;
  }


  public void template() {
    switch (this) {
      case TDrive:
        break;
      case GeoLife:
        break;
      default:
    }
  }

  public static void main(String[] args) {
    System.out.println(createTSBSSchema());
  }
}
