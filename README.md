This repository contains the Java labs as well as their Scala and Python ports of the code used in Manning Publication’s **[Spark in Action, 2nd edition](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp)**, by Jean-Georges Perrin.

---

# Spark in Action, 2nd edition - chapter 2

Welcome to Spark with Java, chapter 2. As we are building our mental model around Spark processing, we need a small but efficient example that does a whole flow.

## Lab

Each chapter has one or more labs. Labs are examples used for teaching in the book(https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#100

The `CsvToDatabaseApp` application does the following:

 1. The app acquires a session (a `SparkSession`).
 1. It then asks Spark to load (ingest) a dataset in CSV format.
 1. Spark performs a small transformation.
 1. Spark stores the result in a database, in this example, it is PostgreSQL.

### Lab \#110

The `CsvToApacheDerbyApp` application is similar to lab \#100, however, instead of using PostgreSQL, it uses [Apache Derby](https://db.apache.org/derby/).

Special thanks to Kelvin Rawls for porting the code to Derby. 

> Apache Derby has a bit of a special place in my heart. Originally named Cloudscape, the eponym company was acquired by [Informix](https://en.wikipedia.org/wiki/IBM_Informix) in 1999, which was itself acquired by [IBM](https://en.wikipedia.org/wiki/IBM) in 2001. Informix's project Arrowhead had the ambition to align the SQL (and other features) between all their databases. Under the new ownership, Cloudscape was influenced by the Db2 SQL before going to the Open Source community as Apache Derby.

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
       
    spark-submit csvToRelationalDatabaseApp.py
   
## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').


1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch02

2. Go to the correct directory:

    cd net.jgp.books.spark.ch02

3. Package application using sbt command

    sbt clean assembly

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --class net.jgp.books.spark.ch02.lab100_csv_to_db.CsvToRelationalDatabaseScalaApp target/scala-2.11/SparkInAction2-Chapter02-assembly-1.0.0.jar

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://facebook.com/sparkinaction/) or in [Manning's live site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
