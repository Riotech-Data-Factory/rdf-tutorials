import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args){
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-solr-indexing")
                .getOrCreate();

        Dataset<Row> moviesDF = sparkSession.read()
                .option("header", "true")
                .csv("hdfs://headnode.bda.rdf.com:9000///user/testbamboo/movies/*.csv")
                .na().fill("N.A.");

        moviesDF.show();

        moviesDF.write()
                .format("solr")
                .option("zkhost", "workernode1.bda.rdf.com:2181")
                .option("collection", "movies")
                .option("soft_commit_secs", "10")
                .save();
    }
}
