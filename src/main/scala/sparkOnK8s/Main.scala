package sparkOnK8s

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkOnK8s.SparkOnK8sJsonSupport.{SparkOnK8sConfiguration, convertConfigFileContentsToObject}
import sparkOnK8s.argumentValidator.ArgumentParser
import sparkOnK8s.core.CoreSpark.{getSchemaFromTableBigQuery, insertIntoBigQueryTable, readSourceFile}
import sparkOnK8s.endtoend.EndToEndTest

object Main extends App with StrictLogging {
  logger.info("Starting SparkOnK8s Application")
  MainBody(args = args).run
  logger.info("Successfully terminated SparkOnK8s Application")
}

case class MainBody(endToEndTestConfigurationFilePath: String,
                    sourceToProcess: String,
                    sparkOnK8sConfiguration: SparkOnK8sConfiguration,
                    sparkSession: SparkSession) extends StrictLogging {

  def run: Unit = {
    //Fail fast -> if destination table does not exists the program will stop
    val tableSchema: Option[StructType] = getSchemaFromTableBigQuery(
      columnsToDrop = Seq(),
      sparkSession = sparkSession,
      table = sparkOnK8sConfiguration.tableName
    )

    tableSchema match {
      case Some(x) =>
        logger.info(s"Table ${sparkOnK8sConfiguration.tableName} exists, reading values from source file ${sourceToProcess}")
        val sourceDataFrame: DataFrame = readSourceFile(
          repartitionFactor = None,
          schema = tableSchema.get,
          sourceFilePath = sourceToProcess,
          sparkSession = sparkSession,
          withHeader = Some(sparkOnK8sConfiguration.withHeader)
        )

        insertIntoBigQueryTable(dataFrame = sourceDataFrame, table = sparkOnK8sConfiguration.tableName)

        endToEndTestConfigurationFilePath.isEmpty match {
          case true =>
            logger.info("Regular run, closing spark session.")

          case false =>
            logger.debug("Daedalus end to end test")
            EndToEndTest.checkHash(
              endToEndTestConfigurationFilePath = endToEndTestConfigurationFilePath,
              sparkSession = sparkSession,
              table = sparkOnK8sConfiguration.tableName
            )
        }
        sparkSession.stop()

      case None =>
        logger.error(s"Error when getting schema from the table ${sparkOnK8sConfiguration.tableName}. Exiting.")
        sparkSession.stop()
        sys.exit(1)
    }
  }
}

object MainBody extends StrictLogging {

  def apply(args: Seq[String]): MainBody = {
    val arguments: ArgumentParser = ArgumentParser.parser.parse(args, ArgumentParser()) match {
      case Some(config) => config
      case None => throw new IllegalArgumentException()
    }
    logger.info("All input arguments were correctly parsed")

    val sparkSession: SparkSession = createSparkSession(
      configs = arguments.configs,
      sparkAppName = arguments.sparkAppName
    )

    val sparkOnK8sConfigFilePath: String = addFileToSparkContext(
      filePath = arguments.sparkOnK8sConfigurationFile,
      sparkSession = sparkSession
    )

    val sparkOnK8sConfiguration: SparkOnK8sConfiguration = convertConfigFileContentsToObject(
      configurationFilePath = sparkOnK8sConfigFilePath)

    val endToEndTestConfigFilePath: String =
      if (arguments.endToEndTestConfigurationFilePath.isEmpty) {
        ""
      }
      else {
        addFileToSparkContext(
          filePath = arguments.endToEndTestConfigurationFilePath,
          sparkSession = sparkSession
        )
      }

    new MainBody(
      endToEndTestConfigurationFilePath = endToEndTestConfigFilePath,
      sourceToProcess = arguments.sourceToProcess,
      sparkOnK8sConfiguration = sparkOnK8sConfiguration,
      sparkSession = sparkSession
    )
  }

  /**
   * Creates a spark session using the (optional) provided configurations.
   *
   * Should the configurations be empty, the default spark session will be returned.
   *
   * @param configs      provided configurations.
   * @param sparkAppName .
   * @return a configured spark session.
   */
  def createSparkSession(configs: Map[String, String], sparkAppName: String): SparkSession = {
    logger.info(s"Creating Spark Session with name $sparkAppName")
    val builder =
      SparkSession
        .builder()
        .appName(sparkAppName)

    val sparkSession: SparkSession = Some(
      configs.foldLeft(builder)((accum, configs) => accum.config(configs._1, configs._2))
    ).getOrElse(builder)
      .getOrCreate()
    sparkSession
  }

  /** Method to add a file to the Spark session.
   *
   * @param filePath     The filepath to the file to add.
   * @param sparkSession The Spark session of the application.
   * @return The file path of the file in the Spark session.
   */
  def addFileToSparkContext(filePath: String, sparkSession: SparkSession): String = {
    sparkSession.sparkContext.addFile(filePath)

    val filePathBasename: String =
      Seq(FilenameUtils.getBaseName(filePath),
        FilenameUtils.getExtension(filePath)).mkString(".")

    SparkFiles.get(filePathBasename)
  }
}
