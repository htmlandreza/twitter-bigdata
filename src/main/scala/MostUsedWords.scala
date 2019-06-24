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
      System.err.println("Usage: MostUsedWords diretorio")
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
      StructField("date", StringType, true) ::
      StructField("text", StringType, true) ::
      StructField("source", StringType, true) ::
      StructField("username", StringType, true) ::
      StructField("hashtags", StringType, true) :: Nil)

    val reader = spark.readStream
      .schema(schema)
      .csv(diretorio)

     val twds = reader
    	.filter(!isnull($"text"))
    	.select($"date" as "data", $"text" as "tweet")
    	.as[Tweet]    

    val words = twds.flatMap(l => l.tweet.split(" "))
      .filter(l => l.length > 2)
      .map(ht => ht.toUpperCase)
      .withColumnRenamed("value", "words")

    val mostUsedWords = words.groupBy("words")
      .count
      .sort($"count".desc)
      .withColumnRenamed("count", "contagem")  

    val query = mostUsedWords.writeStream
      .outputMode(Complete)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("console")
      .start()

    query.awaitTermination()

  }
}