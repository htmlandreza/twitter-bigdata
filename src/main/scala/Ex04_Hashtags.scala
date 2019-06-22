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

object Ex04_Hashtags {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: Ex04_Hashtags diretorio")
      System.exit(1)
    }

    // O shell script copiarTweets.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Ex04_Hashtags")
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
    	.select($"Date" as "data",$"Hour" as "hora",$"Tweet content" as "tweet")
    	.as[Tweet]    
    
    val hashtags = twds.flatMap(t => t.tweet.split(" "))
    	//.filter($"value" like "#%")
    	.filter(l => l.length > 1 && l(0) == '#')
    	.map(ht => ht.toUpperCase)
    	.withColumnRenamed("value","hashtag")
    	
    val contagem = hashtags.groupBy("hashtag")
    	.count
    	.sort($"count".desc)
    	.withColumnRenamed("count","ocorrencias")
    
    val query = contagem.writeStream
      .outputMode(Complete)
      //.outputMode(Update) // Não pode ser usado com sort()
      .trigger(Trigger.ProcessingTime(10.seconds))
      .format("console")
      .start
      
	//query.awaitTermination()	
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
  }
}
