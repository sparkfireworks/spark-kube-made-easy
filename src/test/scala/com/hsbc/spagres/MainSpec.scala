package com.hsbc.spagress

import sparkOnK8s.SparkOnK8sJsonSupport.SpagresConfiguration
import org.scalatest.{Matchers, WordSpec}
import sparkOnK8s.{MainBody, SparkOnK8sJsonSupport}

class MainSpec extends WordSpec with Matchers {
  val testSpagresConfiguration: SpagresConfiguration = SpagresConfiguration(
    tableName = "test_k8s_spark.spagres_e2e_02",
    snapshotIdColumnName = "snapshot_id",
    withHeader = true
  )

  "convertConfigFileContentsToObject" should {
    "convert existing json config file to config object" in {
      val actual: SpagresConfiguration = SparkOnK8sJsonSupport.convertConfigFileContentsToObject(
        configurationFilePath = getClass.getResource("/spagres_config.json").getPath)

      val expected: SpagresConfiguration = testSpagresConfiguration
      assert(expected.equals(actual))
    }

  }

  "generateSnapshotId" should {
    "create a 13 digit sring" in {
      val actual: Int = MainBody.generateSnapshotId().length
      val expected: Int = 17
      assert(actual == expected)
    }

    "last 3 digits are 987 if append is given" in {
      val actual: String = MainBody.generateSnapshotId(append = "987").drop(14)
      val expected: String = "987"
      assert(actual == expected)
    }
  }
}
