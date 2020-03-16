package sparkOnK8s

import com.typesafe.scalalogging.StrictLogging
import sparkOnK8s.core.Core.getFileContents
import spray.json.DefaultJsonProtocol

object SparkOnK8sJsonSupport extends DefaultJsonProtocol with StrictLogging {

  import spray.json._

  implicit val SpagresConfigurationFormat: RootJsonFormat[SpagresConfiguration] = jsonFormat3(SpagresConfiguration)
  implicit val TestProjectDataFormat: RootJsonFormat[TestProjectData] = jsonFormat3(TestProjectData)

  case class SpagresConfiguration(tableName: String,
                                  snapshotIdColumnName: String = "snapshot_id",
                                  withHeader: Boolean)

  case class TestProjectData(columns_order: Array[String],
                             data_frame_hash: Int,
                             order_by_columns: List[String])

  /** Method to convert the contents of a configuration file into an object of type SpagresConfiguration
   *
   * @param configurationFilePath The file path of the configuration.
   * @return Some(SpagresConfiguration) if the file path exists or None if not.
   */
  def convertConfigFileContentsToObject(configurationFilePath: String): SpagresConfiguration = {
    logger.info(s"Converting config file - $configurationFilePath - to object")
    getFileContents(filePath = configurationFilePath)
      .get
      .parseJson
      .convertTo[SpagresConfiguration]
  }

  /** Method to convert the contents of a configuration file into an object of type TestProjectData
   *
   * @param configurationFilePath The file path of the configuration.
   * @return Some(TestProjectData) if the file path exists or None if not.
   */
  def convertEndToEndTestConfigFileContentsToObject(configurationFilePath: String): TestProjectData = {
    logger.info(s"Converting config file - $configurationFilePath - to object")
    getFileContents(filePath = configurationFilePath)
      .get
      .parseJson
      .convertTo[TestProjectData]
  }
}
