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

//Hashtags Mais Usadas por Data
object MostUsedHashtagsDate {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: MostUsedHashtagsDate diretorio")
      System.exit(1)
    }

    // O shell script copiarTweets.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MostUsedHashtagsDate")
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
    	.select( $"date" as "data", 
    	    $"text" as "tweet")
    	.as[Tweet]
    
    val hashtags = twds
    	.flatMap(t => t.tweet.toUpperCase.split(" ").map(p => Tweet(t.data,p)))
    	.filter($"tweet" like "#%")

    val contagem = hashtags.groupBy($"data", $"tweet")
    	.count
    	.select($"data" as "data",$"tweet" as "hashtag",$"count" as "ocorrencias")
  	  .orderBy($"data".desc, $"ocorrencias".desc)
	
    val query = contagem.writeStream
      .outputMode(Complete)
      //.outputMode(Update)
      .format("console")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start

	//query.awaitTermination()	
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
  }
}
