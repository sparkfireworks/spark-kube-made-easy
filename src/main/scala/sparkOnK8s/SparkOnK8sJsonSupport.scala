package sparkOnK8s

import com.typesafe.scalalogging.StrictLogging
import sparkOnK8s.core.Core.getFileContents
import spray.json.DefaultJsonProtocol

object SparkOnK8sJsonSupport extends DefaultJsonProtocol with StrictLogging {

  import spray.json._

  implicit val SparkOnK8sConfigurationFormat: RootJsonFormat[SparkOnK8sConfiguration] = jsonFormat2(SparkOnK8sConfiguration)
  implicit val TestProjectDataFormat: RootJsonFormat[TestProjectData] = jsonFormat3(TestProjectData)

  case class SparkOnK8sConfiguration(tableName: String,
                                     withHeader: Boolean)

  case class TestProjectData(columns_order: Array[String],
                             data_frame_hash: Int,
                             order_by_columns: List[String])

  /** Method to convert the contents of a configuration file into an object of type SparkOnK8sConfiguration
   *
   * @param configurationFilePath The file path of the configuration.
   * @return Some(SparkOnK8sConfiguration) if the file path exists or None if not.
   */
  def convertConfigFileContentsToObject(configurationFilePath: String): SparkOnK8sConfiguration = {
    logger.info(s"Converting config file - $configurationFilePath - to object")
    getFileContents(filePath = configurationFilePath)
      .get
      .parseJson
      .convertTo[SparkOnK8sConfiguration]
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
