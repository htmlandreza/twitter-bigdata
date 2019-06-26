package twitter.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import java.sql.Timestamp

object MostUsedHashtagsDateJSON {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: MostUsedHashtagsDateJSON diretorio")
      System.exit(1)
    }

    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MostUsedHashtagsDateJSON")
      .getOrCreate()

    import spark.implicits._
    
    val schema = StructType(
      StructField("date", StringType, true) ::
			StructField("hour", StringType, true) ::
      StructField("text", StringType, true) ::
      StructField("source", StringType, true) ::
      StructField("username", StringType, true) ::
      StructField("hashtags", StringType, true) :: Nil)

    val leituras = spark.readStream
		.schema(schema)
    	.csv(diretorio)
       
    val twds = leituras
    	.filter(!isnull($"text"))
    	.select( unix_timestamp(
    	    format_string("%s %s:00", $"date", $"hour")
    	    ).cast("timestamp") as "tempo", 
    	    $"text" as "conteudo")
    	.as[TweetWindow]

    val hashtags = twds
    	.flatMap(t => t.conteudo.split(" ").map(p => TweetWindow(t.tempo,p.toUpperCase)))
    	.filter($"conteudo" like "#%")
    	
    val contagem = hashtags
    	.withWatermark("tempo","10 minutes")
    	.groupBy(
    		window($"tempo", "60 minutes", "30 minutes"),
    		$"conteudo"
    	)
    	.count
    	.select($"window.start" as "inicio", $"window.end" as "fim",
    	    $"conteudo", $"count" as "contagem")
          
    val query = contagem.writeStream
      .outputMode(Append)
      .format("json")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .option("path", "/Users/andrezamoreira/Documents/streaming/Realtime/hashtag/json")
      .option("checkpointLocation", "/Users/andrezamoreira/Documents/streaming/Realtime/hashtag/chckpt")
      .start()

    //query.awaitTermination()
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
	
  }
}