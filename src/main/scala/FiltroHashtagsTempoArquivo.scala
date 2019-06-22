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

object FiltroHashtagsTempoArquivo {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: FiltroHashtagsTempoArquivo diretorio")
      System.exit(1)
    }

    // O shell script copiarTweets.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("FiltroHashtagsTempoArquivo")
      .getOrCreate()

    import spark.implicits._
    
    val esquema = StructType(StructField("id", StringType, true) ::
		StructField("date", StringType, true) ::
		StructField("hour", StringType, true) ::
		StructField("username", StringType, true) ::
		StructField("text", StringType, true) ::
		StructField("retweet_count", StringType, true) ::
		StructField("favorite_count", StringType, true) ::
		StructField("source", StringType, true) :: Nil)

    val leituras = spark.readStream
		.schema(esquema)
    	.csv(diretorio)
       
    val twds = leituras
    	.filter(!isnull($"text"))
    	.select( unix_timestamp(
    	    format_string("%s %s:00", $"date", $"hour")
    	    ).cast("timestamp") as "tempo", 
    	    $"text" as "conteudo")
    	.as[TweetWindow]
    
    val hashtags = twds
    	.flatMap(t => t.conteudo.toUpperCase.split(" ").map(p => TweetWindow(t.tempo,p)))
    	.filter($"conteudo" like "#%")
    	
    val contagem = hashtags
    	.withWatermark("tempo","10 minutes")
    	.groupBy(
    		window($"tempo", "60 minutes", "30 minutes"),
    		$"conteudo"
    	)
    	.count
     
    val query = contagem.writeStream
      .outputMode(Append)
      .format("parquet")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .option("path", "/Users/andrezamoreira/Documents/streaming/windowhashtags.parquet")
      .option("checkpointLocation", "/Users/andrezamoreira/Documents/streaming/checkpoint")
      .start()
 
	//query.awaitTermination()
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
	
	/*
	val ht = spark.read.parquet("windowhashtags.parquet")
	ht.orderBy($"window").take(10).foreach(println)
	ht.groupBy("conteudo").agg(sum($"count") as "total").orderBy($"total" desc).show
	ht.groupBy("window").agg(sum($"count") as "total").orderBy($"total" desc).show
	ht.groupBy("window").agg(countDistinct($"conteudo") as "numHashtags").orderBy($"numHashtags" desc).show
	val periodos = ht.groupBy("window").agg(countDistinct($"conteudo") as "numHashtags").orderBy($"numHashtags" desc)
	periodos.take(20).foreach(println)
	*/
  }
}
