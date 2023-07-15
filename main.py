from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = (
    SparkSession
      .builder
      .master("local[*]")
      .appName("PySparkStudy")
      .getOrCreate()
  )

df = (
  spark
  .read
  .option("header", True)
  .csv('./download/pokemon.csv', inferSchema=True)
)

print('Rows number:', df.count())

print("Dataframe Schema:")

df.printSchema()

print("Count Pokemons per type 1")

df.groupBy(f.col("Type 1")).agg(
  f.count(f.col("Name")).alias("Total")
).show(10, False)

