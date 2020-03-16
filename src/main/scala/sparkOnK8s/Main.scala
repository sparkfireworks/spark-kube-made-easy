package sparkOnK8s

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkOnK8s.SparkOnK8sJsonSupport.{SpagresConfiguration, convertConfigFileContentsToObject}
import sparkOnK8s.argumentValidator.ArgumentParser
import sparkOnK8s.core.CoreSpark.{addColumnWithConstantString, getSchemaFromTableBigQuery, insertIntoBigQueryTable, readSourceFile}
import sparkOnK8s.endtoend.EndToEndTest

import scala.util.Random

object Main extends App with StrictLogging {
  logger.info("Starting Spagres Application")
  MainBody(args = args).run
  logger.info("Successfully terminated Spagres Application")
}

case class MainBody(endToEndTestConfigurationFilePath: String,
                    snapshotId: String,
                    sourceToProcess: String,
                    spagresConfiguration: SpagresConfiguration,
                    sparkSession: SparkSession) extends StrictLogging {

  def run: Unit = {
    //Fail fast -> if destination table does not exists the program will stop
    val tableSchema: Option[StructType] = getSchemaFromTableBigQuery(
      columnsToDrop = Seq(this.snapshotId), //It is necessary to drop this column because the snapshot_id is not included in data file
      sparkSession = sparkSession,
      table = spagresConfiguration.tableName
    )

    tableSchema match {
      case Some(x) =>
        logger.info(s"Table ${spagresConfiguration.tableName} exists, reading values from source file ${sourceToProcess}")
        val sourceDataFrame: DataFrame = readSourceFile(
          repartitionFactor = None,
          schema = tableSchema.get,
          sourceFilePath = sourceToProcess,
          sparkSession = sparkSession,
          withHeader = Some(spagresConfiguration.withHeader)
        )

        val dfWithSnapshotId: DataFrame = addColumnWithConstantString(
          columnName = spagresConfiguration.snapshotIdColumnName,
          df = sourceDataFrame,
          value = this.snapshotId
        )

        insertIntoBigQueryTable(dataFrame = dfWithSnapshotId, table = spagresConfiguration.tableName)

        endToEndTestConfigurationFilePath.isEmpty match {
          case true =>
            logger.info("Regular run, closing spark session.")

          case false =>
            logger.debug("Daedalus end to end test")
            EndToEndTest.checkHash(
              endToEndTestConfigurationFilePath = endToEndTestConfigurationFilePath,
              sparkSession = sparkSession,
              table = spagresConfiguration.tableName
            )
        }
        sparkSession.stop()

      case None =>
        logger.error(s"Error when getting schema from the table ${spagresConfiguration.tableName}. Exiting.")
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

    val spagresConfigFilePath: String = addFileToSparkContext(
      filePath = arguments.spagresConfigurationFile,
      sparkSession = sparkSession
    )

    val spagresConfiguration: SpagresConfiguration = convertConfigFileContentsToObject(
      configurationFilePath = spagresConfigFilePath)

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

    val snapshotId: String = arguments.snapshotId match {
      case x if x.isEmpty => generateSnapshotId()
      case x => x
    }

    new MainBody(
      endToEndTestConfigurationFilePath = endToEndTestConfigFilePath,
      snapshotId = snapshotId,
      sourceToProcess = arguments.sourceToProcess,
      spagresConfiguration = spagresConfiguration,
      sparkSession = sparkSession
    )
  }

  def generateSnapshotId(when: Date = Calendar.getInstance().getTime(),
                         append: String = {
                           val r: Random.type = scala.util.Random; { r.nextInt(900) + 100 }.toString
                         }): String = {
    // create the date/time formatters
    val yearFormat: SimpleDateFormat = new SimpleDateFormat("YYYY")
    val monthFormat: SimpleDateFormat = new SimpleDateFormat("MM")
    val dayFormat: SimpleDateFormat = new SimpleDateFormat("dd")
    val minuteFormat: SimpleDateFormat = new SimpleDateFormat("mm")
    val hourFormat: SimpleDateFormat = new SimpleDateFormat("hh")
    val secondFormat: SimpleDateFormat = new SimpleDateFormat("ss")

    val currentYear: String = yearFormat.format(when)
    val currentMonth: String = monthFormat.format(when)
    val currentDay: String = dayFormat.format(when)
    val currentHour: String = hourFormat.format(when)
    val currentMinute: String = minuteFormat.format(when)
    val currentsecond: String = secondFormat.format(when)

    currentYear ++ currentMonth ++ currentDay ++ currentHour ++ currentMinute ++ currentsecond ++ append
  }

  /**
   * Creates a spark session using the (optional) provided configurations.
   *
   * Should the configurations be empty, the default spark session will be returned.
   *
   * @param configs provided configurations.
   * @param sparkAppName.
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
