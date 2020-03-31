package net.jgp.books.spark.ch02.lab900_csv_to_db;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * CSV to a relational database. This is very similar to lab #100, the
 * save() syntax is slightly different.
 * 
 * @author jgp
 */
public class CsvToRelationalDatabaseApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to DB (lab #900)")
        .master("local[*]")
        .getOrCreate();

    // Step 1: Ingestion
    // ---------

    // Reads a CSV file with header, called authors.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/authors.csv");

    // Step 2: Transform
    // ---------

    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column
    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));

    // Step 3: Save
    // ---------

    // Write in a table called ch02lab900
    df.write()
        .mode(SaveMode.Overwrite)
        .option("dbtable", "ch02lab900")
        .option("url", "jdbc:postgresql://localhost/spark_labs")
        .option("driver", "org.postgresql.Driver")
        .option("user", "jgp")
        .option("password", "Spark<3Java")
        .format("jdbc")
        .save();

    System.out.println("Process complete");
  }
}
