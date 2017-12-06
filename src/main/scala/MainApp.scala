import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

case class TransformRule(existingColName: String, newColName: String, newDataType: String, dateExpression: Option[String])
case class OutResult(Column: String, Unique_values: Long, Values: List[(String, Long)])

object MainApp {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Task1Beginner")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  // Register default transformers which allows to convert string values to defined type
  import transformers._
  registerType(StringType)
  registerType(BooleanType)
  registerType(DateType)
  registerType(IntegerType)

  def main(args: Array[String]) {

    args.toList match {
      case csvPath :: rulesPath :: _ => process(csvPath, rulesPath)
      case _ => println("Expected 2 arguments. Run with `sparkdemo csvPath rulesPath`")
    }
  }

  def process(csvPath: String, rulesPath: String): List[OutResult] = {
    // Step #1 load
    val df1 = loadAndFilter(csvPath)

    // Step #2 filter blank strings
    def isNotBlankString(v: Any): Boolean = v match {
      case v: String if v.trim.isEmpty => false
      case _ => true
    }

    val df2 = df1.filter(row => row.toSeq.forall(isNotBlankString))

    // Step #3 transform
    val rulesString = Source.fromFile(rulesPath).getLines().mkString
    val rules = parseRules(rulesString)

    val df3 = transform(df2, rules)

//    println("Filtered DF")
//    df2.show()
//    println("Transformed DF")
//    df3.show()

    // Step #4 gather stats
    val stats = gatherStats(df3)
    println("Show stats " + stats)
    stats
  }

  def loadAndFilter(csvPath: String): DataFrame = sparkSession.read.option("header", "true").csv(csvPath)

  def transform(df: DataFrame, rules: Seq[TransformRule]): DataFrame = {
    import org.apache.spark.sql.functions._

    if (rules.length == 0) {
      return df.select()
    }
    val dropOtherFieldsDf = df.select(rules.map(rule => col(rule.existingColName)):_*)

    rules.foldLeft(dropOtherFieldsDf)((df, rule) => processRule(rule, df))
  }

  private def processRule(rule: TransformRule, df: DataFrame) : DataFrame = {
    val dataType =  CatalystSqlParser.parseDataType(rule.newDataType)

    transformers.withRule(dataType, rule, df)
  }

  def gatherStats(df: DataFrame): List[OutResult] = {
    import org.apache.spark.sql.functions._
    df.cache()

    val exprs = df.columns.map(_ -> "approx_count_distinct").toMap
    val countUniques = df.agg(exprs).take(1)(0)

    def getPerValueCount(df: DataFrame, colName: String): Array[(String, Long)] = {
      df.groupBy(colName).agg(count(col(colName)))
        .filter(col(colName).isNotNull)
        .rdd.map(row => row.get(0).toString -> row.getLong(1))
        .collect()
    }

    df.columns.zipWithIndex
      .map({
        case (name, i) => OutResult(name, countUniques.getLong(i), getPerValueCount(df, name).toList)
      })
      .toList
  }

  def parseRules(rulesRaw: String): Seq[TransformRule] = {
    import org.json4s.native.JsonMethods
    import org.json4s.{DefaultFormats}

    implicit val jsonDefaultFormats = DefaultFormats

    JsonMethods.parse(rulesRaw)
      .camelizeKeys
      .extract[List[TransformRule]]
  }
}