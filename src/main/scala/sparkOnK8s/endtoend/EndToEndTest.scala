package sparkOnK8s.endtoend

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import sparkOnK8s.SparkOnK8sJsonSupport.{TestProjectData, convertEndToEndTestConfigFileContentsToObject}
import sparkOnK8s.core.CoreSpark.{hashedDataFrame, loadBigQueryTableIntoDataFrame}

object EndToEndTest extends StrictLogging {

  def checkHash(endToEndTestConfigurationFilePath: String,
                sparkSession: SparkSession,
                table: String
               ): Unit = {
    logger.info("Starting check Spark end to end results' data frame")
    val testProjectData: TestProjectData = convertEndToEndTestConfigFileContentsToObject(
      configurationFilePath = endToEndTestConfigurationFilePath
    )
    val actual: Int = {
      hashedDataFrame(
        dataFrame = loadBigQueryTableIntoDataFrame(
          sparkSession = sparkSession,
          table = table
        ).select(testProjectData.columns_order.head, testProjectData.columns_order.tail: _*),
        orderByColumns = testProjectData.order_by_columns
      )
    }

    val expected: Int = testProjectData.data_frame_hash

    if (actual == expected) {
      logger.info("End to end Test successful")
    }
    else {
      logger.info(s"Actual data frame hash $actual")
      logger.info(s"Expected data frame hash is $expected")
      logger.error("End to end Test was not successful")
      sys.exit(status = 1)
    }
  }
}

