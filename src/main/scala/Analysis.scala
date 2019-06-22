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
    if (args.length < 1) {
      System.err.println("Usage: Analysis diretorio")
      System.exit(1)
    }

    val diretorio: String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)

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

    //trabalhando com Row, necessário converter para String para efetuar transformações
    val converted = leitura.map(l => l.toString)

    val source = converted.map(l => (l.split(">")(1)))
    .map(l => (l.split("<")(0)))
    .withColumnRenamed("value", "Source")

    source.show(false)

  }
}