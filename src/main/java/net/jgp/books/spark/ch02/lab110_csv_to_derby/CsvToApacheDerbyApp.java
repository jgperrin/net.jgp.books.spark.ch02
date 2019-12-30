package net.jgp.books.spark.ch02.lab110_csv_to_derby;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import net.jgp.books.spark.ch02.x.utils.PrettyFormatter;

/**
 * CSV to a relational database.
 *
 * @author jgp
 */
public class CsvToApacheDerbyApp {

  private static final String JDBC_DRIVER =
      "org.apache.derby.jdbc.ClientDriver";;

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  public static void main(String[] args) {
    CsvToApacheDerbyApp app = new CsvToApacheDerbyApp();

    String user = "admin";
    String password = "adminpass";
    String dbHost = "localhost";
    int dbPort = 1527;
    String dbConnectionUrl =
        "jdbc:derby://" + dbHost + ":" + dbPort + "/spark_labs;create=true";

    try {
      app.startDatabase(dbHost, dbPort);
    } catch (Exception e) {
      System.out.println(
          "Could not initiate database, stopping: " + e.getMessage());
      return;
    }
    app.start(dbConnectionUrl, user, password);
    app.testDatabase(dbConnectionUrl, user, password);
  }

  private void startDatabase(String dbHost, int dbPort) throws Exception {
    // Starts Apache Derby server
    NetworkServerControl server =
        new NetworkServerControl(InetAddress.getByName(dbHost), dbPort);
    server.start(null);
  }

  /**
   * The processing code.
   */
  private void start(String dbConnectionUrl, String user, String password) {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to DB")
        .master("local")
        .getOrCreate();

    // Step 1: Ingestion
    // ---------
    // Reads a CSV file with header, called authors.csv, stores it in a
    // dataframe
    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", "true")
        .load("data/authors.csv");

    // Step 2: Transform
    // ---------
    // Creates a new column called "name" as the concatenation of lname, a
    // virtual column containing ", " and the fname column
    df = df.withColumn(
        "name",
        concat(df.col("lname"), lit(", "), df.col("fname")));
    System.out.println("Result in a dataframe:");
    df.show();

    // Step 3: Save
    // ----
    // The connection URL, assuming your PostgreSQL instance runs locally on
    // the default port, and the database we use is "spark_labs"

    // Properties to connect to the database, the JDBC driver is part of our
    // pom.xml
    Properties prop = new Properties();
    prop.setProperty("driver", JDBC_DRIVER);
    prop.setProperty("user", user);
    prop.setProperty("password", password);

    // Write in a table called ch02
    df.write()
        .mode(SaveMode.Overwrite)
        .jdbc(dbConnectionUrl, "ch02", prop);

    System.out.println("Load Database process complete");
  }

  /**
   * Tests the database output...
   * 
   * @param dbUrl
   * @param dbUser
   * @param dbPassword
   */
  private void testDatabase(String dbUrl, String dbUser,
      String dbPassword) {
    Connection conn = null;
    Statement stmt = null;
    try {
      // STEP 2: Register JDBC driver
      Class.forName(JDBC_DRIVER);

      // STEP 3: Open a connection
      System.out.println("Connecting to Derby");
      conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

      // STEP 4: Execute a query
      stmt = conn.createStatement();

      // STEP 5: Extract data from result set
      System.out.println("Result in Derby:");
      String sql = "SELECT * FROM ch02";
      ResultSet rs = stmt.executeQuery(sql);
      printResults(rs);

      // STEP 6: Clean-up environment
      stmt.close();
      conn.close();
    } catch (SQLException | ClassNotFoundException se) {
      // Handle errors for JDBC
      System.out.println("Errors " + se.getMessage());
    } // Handle errors for Class.forName
    finally {
      // finally block used to close resources
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException se2) {
      } // nothing we can do
      try {
        if (conn != null) {
          conn.close();
        }
      } catch (SQLException se) {
        System.out.println("Errors " + se.getMessage());
      } // end finally try
    } // end try
    System.out.println("Test Database Complete");
  }// end TestDatabase

  /**
   * Prints the results from the resultset
   * 
   * @param rs
   * @throws SQLException
   */
  private void printResults(ResultSet rs) throws SQLException {
    PrettyFormatter pf = new PrettyFormatter();
    pf.set(rs);
    pf.show();

  }
}
