import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ElasticApp {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("basic-app")
      .getOrCreate()

    import sparkSession.implicits._

    val experimentalDF: Dataset[Row] = Seq(("8", "zenon", "siwobrody", "www.siwy.com", 87, "Helloo! I am Zenon siwobrody and I am Pirrrate!"))
      .toDF("id", "firstName", "lastName", "website", "age", "description")

    experimentalDF.write
      .format("org.elasticsearch.spark.sql")
      .option("es.net.ssl", "false")
      .option("es.nodes", "workernode1.bda.rdf.com:9200")
      .mode("Append")
      .option("es.mapping.id", "id")
      .save("experimental-index")
  }
}
