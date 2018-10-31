if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # --------------------------------
    # DataFrame
    
    # spark is an existing SparkSession
    df = spark.read.json("file:///data/spark/examples/src/main/resources/people.json")

    # Displays the content of the DataFrame to stdout
    df.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+

    # Print schema
    df.printSchema()
    # select only the "name"column
    df.select("name").show()
    # select all and increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    # select people older than 21
    df.filter(df['age'] > 21).show()
    # count people by age
    df.groupBy('age').count().show()

    # --------------------------------
    # Spark SQL

    # create a session-scoped temporary view
    df.createOrReplaceTempView('people')

    sqlDF = spark.sql('SELECT * FROM people')
    sqlDF.show()

    # create a global-scope temporary view
    df.createGlobalTempView('people')

    # global temporary view is tied to a system preserved db 'global_temp'
    globalSQLDF = spark.sql('SELECT * FROM global_temp.people')
    globalSQLDF.show()

    # global temporary view is not bound to a session
    spark.newSession().sql('SELECT name, age+1 FROM global_temp.people').show()
