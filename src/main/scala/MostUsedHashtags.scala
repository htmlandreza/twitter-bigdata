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

object MostUsedHashtags {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: MostUsedHashtags diretorio")
      System.exit(1)
    }

    // O shell script copiarTweets.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MostUsedHashtags")
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

    //ajustar 
    val twds = leituras
    	.filter(!isnull($"text"))
    	.select($"date" as "data", $"text" as "tweet")
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
