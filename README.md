The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 2

Welcome to Spark with Java, chapter 2. As we are building our mental model around Spark processing, we need a small but efficient example that does a whole flow.

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).


## Running PySpark

    1. Clone this project
      Assume that cloned this project to ${MY_HOME_DIR}

    2. cd ${MY_HOME_DIR}/src/main/python

    3. Execute the following spark-submit command to create a jar file to our this application
    ```
    spark-submit net/jgp/books/spark/ch02/lab100_csv_to_db/csvToRelationalDatabaseApp.py
    ```
## Running Scala

    1. Clone this project
       Assume that cloned this project to ${MY_HOME_DIR}

    2. cd ${MY_HOME_DIR}

    3. Create application jar file
       ```mvn clean package```

    4. Execute the following spark-submit command to create a jar file to our this application
    ```
    spark-submit --class net.jgp.books.spark.ch02.lab100_csv_to_db.Csv2RelationalDatabaseApp target/sparkInAction2-chapter02-1.0.0-SNAPSHOT.jar
    ```

Notes:
 1. Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10.

---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkWithJava/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
