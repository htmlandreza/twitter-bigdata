package bigdata.mba

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._

object Ex01_NetworkWordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Ex01_NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    //val mestre : String = args(0)
    val host : String = args(0)
    val port : Int = args(1).toInt

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      //.master("local[4]")
      //.master(mestre)
      .appName("Ex01_NetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    val words = lines.as[String].flatMap(_.toUpperCase.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      //.outputMode("update")
      //.outputMode("append") // Não é permitido com groupBy (2.4.0) sem watermark 
      .format("console")
      .start()
      
    //query.awaitTermination()
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
  }
}
