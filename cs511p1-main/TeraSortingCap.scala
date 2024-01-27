import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.SparkConf

object TeraSortingCap {
  def main(args: Array[String]): Unit = {
    //https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession$$Builder.html
    val sparksession = SparkSession.builder.appName("TeraSortingCap").getOrCreate()
    //https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option
    //df.select(df("colA").cast(IntegerType)) https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Column.html#cast(to:org.apache.spark.sql.types.DataType):org.apache.spark.sql.Column
    val mainbody = sparksession.read.option("header", "true").csv("hdfs://main:9000/data/TeraSortingCap.csv").withColumn("Year", col("Year").cast(IntegerType))
    //https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html#filter(conditionExpr:String):org.apache.spark.sql.Dataset[T]
    val removingfuture = mainbody.filter(col("Year") <= 2023)
    val sorteddata = removingfuture.orderBy(desc("Year"), asc("SerialNumber"))
    //https://stackoverflow.com/questions/31674530/write-single-csv-file-using-spark-csv
    sorteddata.coalesce(1).write.option("header", "true").mode("overwrite").csv("hdfs://main:9000/data/sortedterasortingcap")
    //works fine now
    //removingfuture.show()
    //sorteddata.show()
    sparksession.stop()
  }
}
