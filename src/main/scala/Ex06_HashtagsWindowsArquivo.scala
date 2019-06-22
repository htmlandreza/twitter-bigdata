package bigdata.mba

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

object Ex06_HashtagsWindowsArquivo {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: Ex06_HashtagsWindowsArquivo diretorio")
      System.exit(1)
    }

    // O shell script copiarTweets.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("Ex06_HashtagsWindowsArquivo")
      .getOrCreate()

    import spark.implicits._
    
    val esquema = StructType(StructField("Tweet Id", StringType, true) ::
		StructField("Date", StringType, true) ::
		StructField("Hour", StringType, true) ::
		StructField("User Name", StringType, true) ::
		StructField("Nickname", StringType, true) ::
		StructField("Bio", StringType, true) ::
		StructField("Tweet content", StringType, true) ::
		StructField("Favs", StringType, true) ::
		StructField("RTs", StringType, true) ::
		StructField("Latitude", StringType, true) ::
		StructField("Longitude", StringType, true) ::
		StructField("Country", StringType, true) ::
		StructField("Place (as appears on Bio)", StringType, true) ::
		StructField("Profile picture", StringType, true) ::
		StructField("Followers", StringType, true) ::
		StructField("Following", StringType, true) ::
		StructField("Listed", StringType, true) ::
		StructField("Tweet language (ISO 639-1)", StringType, true) ::
		StructField("Tweet Url", StringType, true) :: Nil)

    val leituras = spark.readStream
		.schema(esquema)
    	.csv(diretorio)
       
    val twds = leituras
    	.filter(!isnull($"Tweet content"))
    	.select( unix_timestamp(
    	    format_string("%s %s:00", $"Date", $"Hour")
    	    ).cast("timestamp") as "tempo", 
    	    $"Tweet content" as "conteudo")
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
      .option("path", "/home/assis/streaming/windowhashtags.parquet")
      .option("checkpointLocation", "/home/assis/streaming/checkpoint")
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
