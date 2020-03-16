package sparkOnK8s.core

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object CoreSpark extends StrictLogging {

  /** Method to drop columns of a DataFrame
   *
   * @param dataFrame DataFrame to drop columns of.
   * @param columns   The columns to drop.
   * @return The dataframe with the columns dropped.
   */
  def dropColumns(dataFrame: DataFrame, columns: Seq[String]): DataFrame = {
    logger.debug(s"Dropping columns ${columns.mkString(",")} from DataFrame")
    columns
      .foldLeft(dataFrame)((df: DataFrame, column: String) => if (df.columns.contains(column)) df.drop(column) else df)
  }

  /** Method to get a Spark(StructType) schema from a Big Query table.
   *
   * @param columnsToDrop List of columns to drop from schema.
   * @param sparkSession  The spark session of the application.
   * @param table         The name of the table.
   * @return An Option with the correspondent DataFrame Schema.
   */
  def getSchemaFromTableBigQuery(columnsToDrop: Seq[String] = Seq[String](),
                                 sparkSession: SparkSession,
                                 table: String
                                ): Option[StructType] = {

    logger.info(s"Getting schema from table ${table}")
    Try {
      this.dropColumns(
        this.loadBigQueryTableIntoDataFrame(sparkSession = sparkSession, table = table),
        columns = columnsToDrop
      ).schema
    } match {
      case Success(value) => Some(value)
      case Failure(exception) =>
        logger.error(exception.toString)
        None
    }
  }

  /** Method to create an hash from a DataFrame.
   *
   * @param dataFrame DataFrame to hash.
   * @return Hash of the input DataFrame.
   */
  def hashedDataFrame(dataFrame: DataFrame, orderByColumns: List[String]): Int = {
    val selection: Array[Column] = dataFrame.columns.map(col)
    val joinedValues: String = "joined_values"
    (dataFrame.columns.mkString("_") +
      dataFrame
        .orderBy(orderByColumns.head, orderByColumns.tail: _*)
        .withColumn(joinedValues, concat_ws(sep = "_", selection: _*))
        .select(joinedValues)
        .collect.foldLeft("") { (acc, x) => acc + x(0) }
      ).hashCode
  }

  /** Method to insert data into a Big Query table.
   *
   * @param dataFrame  The DataFrame to insert.
   * @param table      The table to insert into.
   * @param writeMode  The write mode to use.
   * @param partitions The partitions of the table.
   */
  def insertIntoBigQueryTable(dataFrame: DataFrame,
                              table: String,
                              writeMode: String = "append",
                              partitions: Seq[String] = Seq()
                             ): Unit = {
    logger.info(s"Inserting data into Big Query table $table")
    import com.google.cloud.spark.bigquery._
    if (partitions.equals(Seq())) dataFrame.write.mode(writeMode).bigquery(table = table)
    else dataFrame.write.mode(writeMode).partitionBy(partitions: _*).bigquery(table = table)
  }

  /** Method to load a Big Query table into a DataFrame.
   *
   * @param sparkSession The Spark Session of the application.
   * @param table        The name of the table.
   * @return The DataFrame with the table data.
   */
  def loadBigQueryTableIntoDataFrame(sparkSession: SparkSession, table: String): DataFrame = {
    logger.info(s"Loading Big Query table ${table} to DataFrame")
    import com.google.cloud.spark.bigquery._
    sparkSession.read.bigquery(table)
  }

  /** Method to read a csv file to a Dataframe.
   *
   * @param options           Options to use when opening the csv file.
   * @param repartitionFactor Option with the repartition factor for Spark.
   * @param schema            DataFrame Schema.
   * @param sourceFilePath    The file path to the csv file.
   * @param sparkSession      The spark session of the application.
   * @param withHeader        Does the csv file has an header.
   * @return The DataFrame.
   */
  def readSourceFile(options: Option[Map[String, String]] = None,
                     repartitionFactor: Option[Int],
                     schema: StructType,
                     sourceFilePath: String,
                     sparkSession: SparkSession,
                     withHeader: Option[Boolean] = None
                    ): DataFrame = {
    logger.info(s"Reading source file ${sourceFilePath}")

    val withOptions: Map[String, String] = withHeader match {
      case None => options.getOrElse(Map[String, String]())
      case Some(header) => Map("header" -> header.toString) ++ options.getOrElse(Map[String, String]())
    }

    val sourceDataFrame: DataFrame = sparkSession.read.schema(schema).options(withOptions).csv(sourceFilePath)

    repartitionFactor match {
      case None => sourceDataFrame
      case Some(repartitionFactor) => sourceDataFrame.repartition(repartitionFactor)
    }
  }
}
