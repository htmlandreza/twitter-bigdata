//package bigdata.mba

//import org.apache.spark.sql._
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

object Analysis {

  def main(args: Array[String]) {
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Analysis")
      .getOrCreate()

    import spark.implicits._

    /*
     * Para ler do JSON
     *
     * val leitura = spark.read
      .json("json.json")

    leitura.show(false)*/

    val leitura = spark.read
      .option("header", "true")
      .csv("teste.csv")

    leitura.show(false)

    //    val dadosDS = leitura.select($"screen_name" as "username", $"source" as "source", $"text" as "tweet", $"hashtags" as "hashtags").as[Tweet]

    //  dadosDS.show(false)
    //trabalhando com Row, necessário converter para String para efetuar transformaçoes
    val converted = leitura.map(l => l.toString)
    converted.show(false)
    val source = converted.map(l => (l.split(">")(1)))
      .map(l => (l.split("<")(0)))
      .withColumnRenamed("value", "source")

    val mostUsedSources = source.groupBy("source")
      .count
      .withColumnRenamed("value", "source")
      .orderBy($"count".desc)
      .show

    val words = converted.map(l => (l.split(",")(2)))
      .map(l => l.split(" "))
      .filter(l => l.length > 2)
      .withColumn("value", explode($"value"))
      .withColumnRenamed("value", "words")

    words.show(false)

    val mostUsedWords = words.groupBy("words")
      .count
      .withColumnRenamed("value", "words")
      .orderBy($"count".desc)
      .show
  }
}