import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.DataTypes
import udfs.ChangeStringToLongUDF

object App {
  def main(args: Array[String]): Unit ={
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("basic-app")
      .getOrCreate()

    val changeStringToLongUDF: ChangeStringToLongUDF = new ChangeStringToLongUDF()

    sparkSession.udf.register("changeStringToLongUDF", changeStringToLongUDF, DataTypes.LongType)

    val moviesDF: Dataset[Row] = sparkSession.read
      .option("header", "true")
      .csv("hdfs://headnode.bda.rdf.com:9000///user/testbamboo/movies/*.csv")
      .na.fill("N.A.")

    val moviesWithProperValues: Dataset[Row] = moviesDF.withColumn("id", callUDF("changeStringToLongUDF", col("id")))
      .withColumn("revenue", callUDF("changeStringToLongUDF", col("revenue")))
      .na.drop(Seq("id", "revenue"))

    val moviesCount: Long = moviesWithProperValues.count()

    moviesWithProperValues.show(250)
    moviesWithProperValues.printSchema()
    println(s"COUNT: $moviesCount")

    moviesWithProperValues.write
      .parquet("hdfs://headnode.bda.rdf.com:9000///user/testbamboo/movies/allMovies.parquet")
  }
}
