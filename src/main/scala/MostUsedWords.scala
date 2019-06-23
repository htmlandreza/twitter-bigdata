package twitter.bigdata

import org.apache.spark.sql.{ Dataset, Row, _ }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object MostUsedWords {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: AnalysisStreaming diretorio")
      System.exit(1)
    }

    val diretorio: String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MostUsedWords")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(
      StructField("screen_name", StringType, true) ::
        StructField("source", StringType, true) ::
        StructField("text", StringType, true) ::
        StructField("hashtags", StringType, true) :: Nil)

    val reader = spark.readStream
      .schema(schema)
      .csv(diretorio)

    val converted = reader.map(l => l.toString)

    val words = converted.map(l => (l.split(",")(2)))
      .map(l => l.split(" "))
      .filter(l => l.length > 2)
      .withColumn("value", explode($"value"))
      .withColumnRenamed("value", "words")

    val mostUsedWords = words.groupBy("words")
      .count
      .withColumnRenamed("value", "words")
      .orderBy($"count".desc)

    val query = mostUsedWords.writeStream
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()

  }
}