//Prepare input data by doing some ETL operations
//Somewhat specific to walmart poc data

package aijusProd

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{StructField, StructType, _}

object Preparation {
  //Get context created in main method to read files
  val sc: SparkContext = SparkContext.getOrCreate()
  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

  import spark.implicits._

  //Defines levels of classification variable
  val procedencia = List("LIQUIDAÇÃO DO PROCESSO", "AÇÃO JULGADA PARCIALMENTE PROCEDENTE", "PAGTO DE CONDENAÇÃO",
    "AÇÃO JULGADA TOTALMENTE PROCEDENTE", "PEDIDO JULGADO PARCIALMENTE PROCEDENTE", "PAGAMENTO", "PERDA TOTAL")
  val improcedenciaSJulg = List("INCOMPETENCIA DE LUGAR", "DESISTÊNCIA DA AÇÃO", "LIQUIDAÇÃO DO PROCESSO PELA 1ª " +
    "RECLAMADA", "WM EXCLUÍDO DA LIDE", "INCOMPETENCIA EM RAZAO DA MATERIA", "ACORDO COM A 1ª RECLAMADA", "ACORDO COM" +
    " CORRÉU", "AUTOS REMETIDOS PARA OUTRA REGIÃO", "PAGAMENTO PELO CO-RÉU", "INERCIA DO RECLAMANTE", "EXCLUSÃO DO PÓLO")
  val improcedenciaCJulg = List("AÇÃO JULGADA IMPROCEDENTE", "PEDIDO JULGADO IMPROCEDENTE")
  val improcedencia: List[String] = improcedenciaSJulg.union(improcedenciaCJulg)
  val acordo = List("ACORDO HOMOLOGADO", "CONCILIADOS")

  //Verifies in which case the label matches
  def defineLabel(encerramento: String, splitLabel: Boolean): String = {
    encerramento match {
      case `encerramento` if acordo.contains(encerramento) => "acordo"
      case `encerramento` if procedencia.contains(encerramento) => "procedente"
      //if (splitLabel){
      case `encerramento` if splitLabel && improcedenciaSJulg.contains(encerramento) => "improcedente sem julgamento"
      case `encerramento` if splitLabel && improcedenciaCJulg.contains(encerramento) => "improcedente com julgamento"
      //} else {
      case `encerramento` if (!splitLabel) && improcedencia.contains(encerramento) => "improcedente"
      //}
      case _ => null
    }
  }

  //Transforms values fields that are in Brazilian notation (1.000,00) into regular floats
  def fixRealValues(df: DataFrame, columns: List[String]): DataFrame = {
    var aux = df
    columns.foreach(colum =>
      aux = aux.withColumn(colum + "_sem_dot", regexp_replace(col(colum), "\\.", ""))
        .withColumn(colum + "_sem_comma", regexp_replace(col(colum + "_sem_dot"), ",", "\\.").cast("float"))
        .drop(colum, colum + "_sem_dot")
        .withColumnRenamed(colum + "_sem_comma", colum)
    )
    val output = aux
    output
  }

  //Deflate values to the present currency using ipca index
  def deflateValues(df: DataFrame, columns: List[String]): DataFrame = {
    var aux = df
    columns.foreach(colum =>
      aux = aux.withColumn(colum + "_deflated", col(colum)*col("idx"))
        .drop(colum)
        .withColumnRenamed(colum + "_deflated", colum)
    )
    val output = aux
    output
  }

  //ETL of wallmart poc data
  def prepData(splitLabel: Boolean): (DataFrame,DataFrame)= {
    val udfDefineLabel = udf(defineLabel _)

    val encerrados = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/christopher/Documents/Juridico/Juribot/pocWalmart/data/1_Encerrados/1_Processos/Base_Encerrados_Jud_01_2010_Brasil_Com_CPF.csv")
      .withColumnRenamed("Participacao4", "Participacao Adversa")
      .withColumn("Data De Admissao New", from_unixtime(unix_timestamp($"DATA DE ADMISSAO", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .withColumn("Data De Demissao New", from_unixtime(unix_timestamp($"DATA DE DEMISSAO", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .drop("DATA DE ADMISSAO","DATA DE DEMISSAO")
      .withColumnRenamed("Data De Admissao New","Data De Admissao")
      .withColumnRenamed("Data De Demissao New","Data De Demissao")
      .withColumn("uf",when($"UF" === "\\-",lit(null)).otherwise($"UF"))
      .drop("UF")

    val ativos = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/christopher/Documents/Juridico/Juribot/pocWalmart/data/3_Ativos/1_Processos/Base_Ativo_Jud_Brasil_Com_CPF.csv")
      .withColumnRenamed("Participacao4", "Participacao Adversa")
      .withColumn("Date Ajuiz",from_unixtime(unix_timestamp($"Data de Ajuizamento", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .withColumn("Data De Admissao New", from_unixtime(unix_timestamp($"DATA DE ADMISSAO", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .withColumn("Data De Demissao New", from_unixtime(unix_timestamp($"DATA DE DEMISSAO", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .drop("DATA DE ADMISSAO","DATA DE DEMISSAO","Data de Ajuizamento")
      .withColumnRenamed("Data De Admissao New","Data De Admissao")
      .withColumnRenamed("Data De Demissao New","Data De Demissao")
      .withColumn("uf",when($"UF" === "\\-",lit(null)).otherwise($"UF"))
      .drop("UF")
      .withColumn("ano",year($"Date Ajuiz"))

    val compAtivos = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load("/home/christopher/Documents/Juridico/Juribot/pocWalmart/data/3_Ativos/1_Processos/Complemento_Processos_Ativos.csv")
      .drop("Participacao")

    val dateAjuizamento = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/christopher/Documents/Juridico/Juribot/pocWalmart/data/1_Encerrados/1_Processos/Data Ajuizamento - Encerramos - Pasta(1).csv")
      .withColumn("Date Ajuiz", from_unixtime(unix_timestamp($"Data de Ajuizamento", "dd/MM/yyyy"), "yyyy-MM-dd").cast("date"))
      .drop("Data de Ajuizamento")
      .withColumn("ano",year($"Date Ajuiz"))

    val dateEncerramento = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("/home/christopher/Documents/Juridico/Juribot/pocWalmart/data/1_Encerrados/1_Processos/Data de Encerramentos -.csv")
      .withColumn("Date Enc", from_unixtime(unix_timestamp($"Data de encerramento", "dd/MM/yyyy"), "yyyy-MM-dd").cast
      ("date"))
      .withColumn("Date Enc Mes", concat(year($"Date Enc"), lit("-"), month($"Date Enc"), lit("-01")).cast("date"))
      .drop("Data de encerramento")
      .drop("Motivo de encerramento")

    val ativosComp = ativos.join(compAtivos, Seq("Pasta"), "left")

    val namesAtivos = ativosComp.columns
      .map(_.toLowerCase)
      .map(StringUtils.stripAccents)
      .map(_.replaceAll("""\p{Punct}""", ""))
      .map(_.replaceAll("""[^a-zA-Z0-9\s]""", ""))
      .map(_.replaceAll("""[\s]""", "_"))

    val ativosRenamed = ativosComp.toDF(namesAtivos: _*)
      .withColumn("date_enc", lit("2017-08-01").cast("date"))
      .withColumn("date_enc_mes", concat(year($"date_enc"), lit("-"), month($"date_enc"), lit("-01")).cast("date"))
      .withColumnRenamed("valor_oca", "valor_provavel_oca")

    val encerrados_date = encerrados.join(dateAjuizamento, Seq("Pasta"), "left")
      .join(dateEncerramento, Seq("Pasta"), "left_outer")

    val namesEncerrados = encerrados_date.columns
      .map(_.toLowerCase)
      .map(x => StringUtils.stripAccents(x))
      .map(_.replaceAll("""\p{Punct}""", ""))
      .map(_.replaceAll("""[^a-zA-Z0-9\s]""", ""))
      .map(_.replaceAll("""[\s]""", "_"))

    val encerradosRenamed = encerrados_date.toDF(namesEncerrados: _*)
      .drop("empresa__razao_social",
        "vlr_prov_atual_wm_possivel",
        "vlr_prov_atual_wm_remota",
        "centro_de_custo_wm_atualizado",
        "c_custo_wm",
        "valor_oca_atu")

    val colsAtivos = ativosRenamed.columns.intersect(encerradosRenamed.columns)

    val ativosCols = ativosRenamed.select(colsAtivos.head, colsAtivos.tail: _*)

    val ativosSemEComm = ativosCols.filter(!$"bandeira".contains("COMMERCE"))
      .withColumn("tempo_processo", months_between($"date_enc", $"date_ajuiz"))
      //.withColumn("tempo_trabalhado", months_between($"data_de_demissao", $"data_de_admissao"))

    val encerradosSemEComm = encerradosRenamed.filter((!$"bandeira".contains("COMMERCE")) && (!$"bandeira".contains("CADASTRO")))

    val encerradosLabeled = encerradosSemEComm
      .withColumn("label", udfDefineLabel($"motivo_de_encerramento",lit(splitLabel)))
      .withColumn("tempo_processo", months_between($"date_enc", $"date_ajuiz"))
      //.withColumn("tempo_trabalhado", months_between($"data_de_demissao", $"data_de_admissao"))

    val valores = List("valor_da_causa",
      "valor_provavel_oca",
      "valor_provisao_wm_possivel",
      "valor_provisao_wm_remota",
      "valor_oca_correcao",
      "valor_oca_juros")

    val ativosFloat = fixRealValues(ativosSemEComm, valores)
    val encerradosFloat = fixRealValues(encerradosLabeled, valores)

    val ipcaSchema = StructType(Array(
      StructField("data", DateType, nullable = true),
      StructField("ipca", DoubleType, nullable = true),
      StructField("idx", DoubleType, nullable = true)))

    val ipca = spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .schema(ipcaSchema)
      .load("/home/christopher/Downloads/ipca.csv")
      .drop("ipca")

    val ativosIPCA = ativosFloat.join(ipca, ativosFloat.col("date_enc_mes") === ipca.col("data"), "left")
        .drop("data")
    val encerradosIPCA = encerradosFloat.join(ipca, encerradosFloat.col("date_enc_mes") === ipca.col("data"), "left")
        .drop("data")

    val ativosDeflated = deflateValues(ativosIPCA, valores)
        .drop("idx","date_enc_mes")
    val encerradosDeflated = deflateValues(encerradosIPCA, valores)
      .drop("idx","date_enc_mes")

    /////////////////////////////////////////////////////////////////////////////
    //                        Falta o Agrupamento Manual
    /////////////////////////////////////////////////////////////////////////////

    val encerradosFiltrado = encerradosDeflated.filter($"label".isNotNull)
        .filter($"tempo_processo" >= 0)

    val ativosFiltrado = ativosDeflated.filter($"tempo_processo" >= 0)

    (ativosFiltrado,encerradosFiltrado)

  }
}
