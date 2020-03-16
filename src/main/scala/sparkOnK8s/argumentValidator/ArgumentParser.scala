package sparkOnK8s.argumentValidator

import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}

import scopt.OptionParser

/** Case class for the config object.
 *
 * @param configs                           Optional Spark configurations.
 * @param endToEndTestConfigurationFilePath The path to the end to end test.
 * @param sourceToProcess                   File or or directory to process.
 * @param sparkOnK8sConfigurationFile       The path to the JSON file with the configuration.
 * @param sparkAppName                      SparkOnK8s Spark application name.
 */
case class ArgumentParser(configs: Map[String, String] = Map(),
                          endToEndTestConfigurationFilePath: String = "",
                          sourceToProcess: String = "",
                          sparkOnK8sConfigurationFile: String = "",
                          sparkAppName: String = "SparkOnK8s Spark")

/** Object that parses the arguments received. */
object ArgumentParser {

  val parser: OptionParser[ArgumentParser] =
    new scopt.OptionParser[ArgumentParser](programName = "Spark") {
      head(xs = "SparkOnK8s")

      opt[String]('n', name = "sparkAppName")
        .valueName("SparkOnK8s Spark Application Name")
        .optional()
        .action((x, c) => c.copy(sparkAppName = x))
        .text("Spark application name.")

      opt[String]('s', name = "sourceToProcess")
        .valueName("fileToProcess.csv or directory to process")
        .required()
        .action((x, c) => c.copy(sourceToProcess = x))
        .validate(x => if (validateConfigFileExistance(value = x)) {
          success
        } else {
          failure(msg = "File/Directory to process does not exist")
        })
        .text("File/Directory to process")


      opt[String]('j', name = "configurationFile")
        .valueName("configuration_file.json")
        .required()
        .action((x, c) => c.copy(sparkOnK8sConfigurationFile = x))
        .validate(x => if (validateConfigFileExistance(value = x)) {
          success
        } else {
          failure(msg = "JSON file does not exist")
        })
        .text("Service Config File")

      opt[Map[String, String]]('c', name = "configs")
        .valueName("configs")
        .optional()
        .action((x, c) => c.copy(configs = x))
        .text("configs")

      opt[String]('t', name = "endToEndConfigurationFile")
        .valueName("endToEndConfigurationFile")
        .action((x, c) => c.copy(endToEndTestConfigurationFilePath = x))
        .validate(x => if (validateConfigFileExistance(value = x)) {
          success
        } else {
          failure(msg = "End 2 end test JSON file does not exits")
        })
        .text("endToEndConfigurationFile")
    }

  /** Method to check if the json file received exists.
   *
   * @param filePath the json file to check.
   * @return true if the file exists, false if not.
   */
  def isLocalFileExists(filePath: String): Boolean = {
    Files.exists(Paths.get(filePath))
  }

  /** Method that checks if a remote address exists and is reachable.
   *
   * @param address The address to check.
   * @return True if the address exists, false if not.
   */
  def isRemoteAddressExists(address: String): Boolean = {
    val url: URL = new URL(address.replaceFirst("gs://", "https://storage.googleapis.com/"))
    val httpUrlConnection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
    httpUrlConnection.setRequestMethod("HEAD")
    httpUrlConnection.getResponseCode == HttpURLConnection.HTTP_OK
  }

  /** Method to check if the value passed as argument exists, either locally or remotely.
   *
   * @param value The argument to check.
   * @return True if the file exists, false if not.
   */
  def validateConfigFileExistance(value: String): Boolean = {
    isLocalFileExists(filePath = value) || isRemoteAddressExists(address = value)
  }
}
