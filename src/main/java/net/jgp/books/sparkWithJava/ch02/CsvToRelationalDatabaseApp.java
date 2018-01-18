package net.jgp.books.sparkWithJava.ch02;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV to a relational database.
 * 
 * @author jperrin
 */
public class CsvToRelationalDatabaseApp {

	/**
	 * main() is your entry point to the application. 
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
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header, called books.csv, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");

        // Shows at most 5 rows from the dataframe
        df.show(5);
    }
}
