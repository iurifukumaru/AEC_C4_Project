package bixi

import org.apache.hadoop.fs.Path

object Main extends App with HDFS {

  if(fs.delete(new Path(s"$uri/user/winter2020/iuri/bixi"),true))
    println("Folder 'bixi - project' deleted before Instantiate!")

  fs.mkdirs(new Path(s"$uri/user/winter2020/iuri/bixi"))

  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/FinalProject/system_alerts.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/system_alerts/system_alerts.json"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/FinalProject/system_information.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/system_information/system_information.json"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/FinalProject/station_status.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/station_status/station_status.json"))
  fs.copyFromLocalFile(new Path("file:///home/iuri/Documents/FinalProject/station_information.json"),
    new Path(s"$uri/user/winter2020/iuri/bixi/station_information/station_information.json"))

  stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
  stmt.executeUpdate("""CREATE DATABASE IF NOT EXISTS winter2020_iuri""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_system_alerts_v4""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_system_information_v4""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_station_status_v4""".stripMargin)
  stmt.executeUpdate("""DROP TABLE ext_station_information_v4""".stripMargin)
  stmt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE ext_system_alerts_v4 (
      |last_updated STRING,
      |ttl SMALLINT,
      |data STRUCT<
      |  alerts: ARRAY< STRUCT<
      |    alert_id: STRING,
      |    type: STRING,
      |    times: ARRAY<STRUCT<
      |      `start`: STRING,
      |      `end`: STRING>>,
      |    station_ids: ARRAY<STRING>,
      |    region_ids: ARRAY<STRING>,
      |    url: STRING,
      |    summary: STRING,
      |    description: STRING,
      |    last_updated: STRING
      |  >>
      |>)
      |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/iuri/bixi/system_alerts/'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE ext_system_information_v4 (
      |last_updated STRING,
      |ttl SMALLINT,
      |data STRUCT<
      |  system_id:	STRING,
      |  language: STRING,
      |  name: STRING,
      |  short_name: STRING,
      |  operator: STRING,
      |  url: STRING,
      |  purchase_url: STRING,
      |  start_date: STRING,
      |  phone_number: STRING,
      |  email:	STRING,
      |  feed_contact_email: STRING,
      |  timezone: STRING,
      |  license_id: STRING,
      |  license_url: STRING,
      |  attribution_organization_name: STRING,
      |  attribution_url: STRING,
      |  rental_apps: STRUCT<
      |    android: STRUCT<
      |      store_uri: STRING,
      |      discovery_uri: STRING
      |    >,
      |    ios: STRUCT<
      |      store_uri: STRING,
      |      discovery_uri: STRING
      |    >
      |  >
      |>)
      |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/iuri/bixi/system_information/'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE ext_station_status_v4 (
      |last_updated STRING,
      |ttl SMALLINT,
      |data STRUCT<
      |  stations: ARRAY< STRUCT<
      |    station_id: STRING,
      |    num_bikes_available: INT,
      |    num_bikes_disabled: INT,
      |    num_docks_available: INT,
      |    num_docks_disabled: INT,
      |    is_installed: BOOLEAN,
      |    is_renting: BOOLEAN,
      |    is_returning: BOOLEAN,
      |    last_reported: BOOLEAN,
      |    vehicle_docks_available: ARRAY <STRUCT<
      |      vehicle_type_ids: ARRAY<STRING>,
      |      `count` : INT
      |    >>,
      |    vehicles: ARRAY <STRUCT<
      |      bike_id: STRING,
      |      is_reserved: BOOLEAN,
      |      is_disabled: BOOLEAN,
      |      vehicle_type_id: STRING,
      |      current_range_meters: FLOAT
      |    >>
      |  >>
      |>)
      |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/iuri/bixi/station_status/'
      |""".stripMargin
  )

  stmt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE ext_station_information_v4 (
      |last_updated STRING,
      |ttl SMALLINT,
      |data STRUCT<
      |  stations: ARRAY<STRUCT<
      |    station_id: STRING,
      |    name: STRING,
      |    short_name: STRING,
      |    lat: FLOAT,
      |    lon: FLOAT,
      |    address: STRING,
      |    cross_street: STRING,
      |    region_id: STRING,
      |    post_code: STRING,
      |    rental_methods: ARRAY<STRING>,
      |    is_virtual_station: BOOLEAN,
      |    station_area: STRING,
      |    capacity: INT,
      |    vehicle_capacity: STRING,
      |    is_valet_station: BOOLEAN,
      |    rental_uris: STRUCT<
      |      android: STRING,
      |      ios: STRING,
      |      web: STRING>,
      |    vehicle_type_capacity: STRING
      |  >>
      |>)
      |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      |STORED AS TEXTFILE
      |LOCATION '/user/winter2020/iuri/bixi/station_information/'
      |""".stripMargin
  )

}
