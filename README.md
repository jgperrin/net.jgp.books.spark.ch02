The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition - chapter 2

Welcome to Spark with Java, chapter 2. As we are building our mental model around Spark processing, we need a small but efficient example that does a whole flow.

## Running the lab in Java

For information on running the Java lab, see chapter 2 in [Spark in Action, 2nd edition](http://jgp.net/sia).


## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project
    
   git clone https://github.com/jgperrin/net.jgp.books.spark.ch02

2. Go to the lab in the Python directory
    
   cd net.jgp.books.spark.ch02/src/main/python/lab100_csv_to_db/

3. Execute the following spark-submit command to create a jar file to our this application
       
   ```
     spark-submit csvToRelationalDatabaseApp.py
   ```
   
## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').


1. Clone this project
       
   git clone https://github.com/jgperrin/net.jgp.books.spark.ch02

2. cd net.jgp.books.spark.ch02

3. Package application using sbt command

   ```
      sbt clean assembly
   ```

4. Run Spark/Scala application using spark-submit command as shown below:
   
    ```
    spark-submit --class net.jgp.books.spark.ch02.lab100_csv_to_db.CsvToRelationalDatabaseScalaApp target/scala-2.11/SparkInAction2-Chapter02-assembly-1.0.0.jar
    ```

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkWithJava/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
