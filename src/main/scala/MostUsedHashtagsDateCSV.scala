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

object MostUsedHashtagsDateCSV {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: MostUsedHashtagsDateCSV diretorio")
      System.exit(1)
    }

    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("MostUsedHashtagsDateCSV")
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
    	    format_string("%s:00", $"hour")
    	    ).cast("timestamp") as "tempo", 
    	    $"text" as "conteudo")
    	.as[TweetWindow]

    val hashtags = twds
    	.flatMap(t => t.conteudo.split(" ").map(p => TweetWindow(t.tempo,p.toUpperCase)))
    	.filter($"conteudo" like "#%")
    	
    val contagem = hashtags
    	.withWatermark("tempo","10 minutes")
    	.groupBy(
    		window($"tempo", "30 minutes", "15 minutes"),
    		$"conteudo"
    	)
    	.count
    	//.select($"window".apply("start") as "inicio", $"window".apply("end") as "fim", 
    	//    $"conteudo", $"count" as "contagem")
    	.select($"window.start" as "inicio", $"window.end" as "fim", 
    	    $"conteudo", $"count" as "contagem")

    val query = contagem.writeStream
      .outputMode(Append)
      .format("csv")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .option("path", "/Users/andrezamoreira/Documents/streaming/Realtime/hashtag")
      .option("checkpointLocation", "/Users/andrezamoreira/Documents/streaming/Realtime/hashtag/log")
      .start()

    //query.awaitTermination()
	while (scala.io.StdIn.readLine("X + [ENTER] para sair! ").trim.toUpperCase != "X"){
	}
	println("Encerrando...")
	query.stop()
	println("Fim!")
	
      /* shell:
      import org.apache.spark.sql.types._
      import java.sql.Timestamp
      import spark.implicits._
      case class TweetWindow(inico:java.sql.Timestamp, fim:java.sql.Timestamp, conteudo:String, contagem:Int)
      Com Strings: val esq = new StructType().add("inicio", StringType).add("fim", StringType).add("conteudo",StringType).add("contagem", IntegerType)
      val esq = new StructType().add("inicio", TimestampType).add("fim", TimestampType).add("conteudo",StringType).add("contagem", IntegerType)
      val df = spark.read.schema(esq).csv("windowhashtags.csv")
      df.orderBy($"inicio").take(100).foreach(println)
      df.createOrReplaceTempView("hashtag")
      spark.sql("select conteudo, sum(contagem) as total from hashtag group by conteudo order by total desc limit 10").show
      spark.sql("select inicio, fim, conteudo, contagem from hashtag h1 where contagem = (select max(contagem) from hashtag h2 where h2.inicio = h1.inicio and h2.fim = h1.fim) order by inicio").show
      */      
  }
}