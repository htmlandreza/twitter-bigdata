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

object Ex03_DiretorioLocalStream {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: Ex03_DiretorioLocalStream diretorio")
      System.exit(1)
    }

    // O shell script copiarAtividades.sh deve ser invocado 
    // para alimentar o diretório com arquivos pouco a pouco
    
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      //.master("local[*]")
      .appName("Ex03_DiretorioLocalStream")
      .getOrCreate()

    import spark.implicits._
    
    val esquema = new StructType()
      	.add("usuario","int")
    	.add("atividade","string")
    	.add("cp3","string")
    	.add("cp4","string")
    	.add("cp5","string")
    	.add("cp6","string")

    val leituras = spark.readStream
		.schema(esquema)
    	.csv(diretorio)
       
    val atividades = leituras.select($"usuario",$"atividade").as[Leitura]
    
    
    // Contagem por usuário e tipo de atividade
    val contagens = atividades.groupBy("usuario","atividade")
    	.count
    	.withColumnRenamed("count","leituras")
    	.orderBy($"usuario",$"leituras".desc)
    
    /*
    // Contagem por tipo de atividade
    val contagens = atividades.groupBy("atividade")
    	.count
    	.withColumnRenamed("count","leituras")
    	.orderBy($"leituras".desc)
	*/
    
    val query = contagens.writeStream
      .outputMode(Complete)
      //.outputMode(Update) // Não pode ser usado com orderBy()
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
