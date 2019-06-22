package bigdata.mba

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._

object Ex02_DiretorioLocalFixo {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: Ex02_DiretorioLocalFixo diretorio")
      System.exit(1)
    }

    //val mestre : String = args(0)
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      //.master("local[*]")
      //.master(mestre)
      .appName("Ex02_DiretorioLocalFixo")
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
		.option("maxFilesPerTrigger", 1)
    	.csv(diretorio)
       
    val atividades = leituras
    	.select("usuario","atividade").as[Leitura]
    
       
    val contagens = atividades.groupBy("usuario","atividade")
    	.count
    	.withColumnRenamed("count","leituras")
    	.orderBy($"usuario",$"leituras".desc)
    
    /*
    val contagens = atividades.groupByKey(l => l)
    	.count // resulta em um Dataset de Tuple2 -> (chave, count)
    	.map(elem => LeituraContagem(elem._1.usuario, elem._1.atividade ,elem._2))
    	.orderBy($"usuario",$"leituras".desc)
    */
    	
    val query = contagens.writeStream
      .outputMode(Complete)
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
