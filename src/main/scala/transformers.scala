import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * Define type class for transform column type logic
  * Usage:
  * 1. Implement column transformer to specific type by extending {{ColumnTransformer}}
  * 2. Register it via {{registerType(NewType)(newTypeTransformerInstance)}}
  *
  * Example:
  * ```
  * implicit val stringTransformer: ColumnTransformer[StringType.type] = ApplyNativeRule()
  * registerType(StringType)
  * ```
  */
object transformers {

  trait ColumnTransformer[A] {
    def transform(dataType: A, rule: TransformRule, df: DataFrame): DataFrame
    def transform(rule: TransformRule, df: DataFrame): DataFrame
  }

//  private def withRule[A](dataType: A, rule : TransformRule, df: DataFrame)(implicit columnTransformer: ColumnTransformer[A]): DataFrame =
//    columnTransformer.transform(dataType, rule, df)

  object ApplyCustomCast {
    def apply[A](dataType: A, func: (TransformRule, DataFrame) => DataFrame) = new ColumnTransformer[A] {
      override def transform(dataType: A, rule: TransformRule, df: DataFrame): DataFrame = func(rule, df)
      override def transform(rule: TransformRule, df: DataFrame): DataFrame = func(rule, df)
    }
  }

  object ApplyNativeCast {
    def apply[A]() = new ColumnTransformer[A] {
      override def transform(dataType: A, rule: TransformRule, df: DataFrame): DataFrame = transform(rule, df)
      override def transform(rule: TransformRule, df: DataFrame): DataFrame = df
        .withColumnRenamed(rule.existingColName, rule.newColName)
        .withColumn(rule.newColName, col(rule.newColName).cast(rule.newDataType))
    }
  }

  implicit val stringTransformer: ColumnTransformer[StringType.type] = ApplyNativeCast()
  implicit val integerTransformer: ColumnTransformer[IntegerType.type] = ApplyNativeCast()
  implicit val booleanTransformer: ColumnTransformer[BooleanType.type] = ApplyNativeCast()

  implicit val dateTransformer: ColumnTransformer[DateType.type] =
    ApplyCustomCast(DateType, (rule, df) => df
      .withColumnRenamed(rule.existingColName, rule.newColName)
      .withColumn(rule.newColName, to_date(col(rule.newColName), rule.dateExpression.getOrElse("ddMMyyyy")))
    )


  private val registeredTypes = mutable.Map.empty[Class[_ <: DataType], ColumnTransformer[_]]

  def registerType[A <: DataType](dataType: A)(implicit transformer: ColumnTransformer[A]): Unit = {
    registeredTypes += (dataType.getClass -> transformer)
  }

  def withRule(dataType: DataType, rule : TransformRule, df: DataFrame): DataFrame = {
    registeredTypes.get(dataType.getClass) match {
      case Some(transformer: ColumnTransformer[_]) => transformer.transform(rule, df)
      case _ => throw new RuntimeException(s"Data type is not supported: ${rule.newDataType}. Please register with: `transformers.registerType(NewType)`")
    }
  }
}




