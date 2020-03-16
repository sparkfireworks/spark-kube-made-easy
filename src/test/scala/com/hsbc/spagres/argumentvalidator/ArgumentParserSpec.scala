package com.hsbc.spagres.argumentvalidator

import sparkOnK8s.argumentValidator.ArgumentParser
import org.scalatest.{Matchers, WordSpec}

class ArgumentParserSpec extends WordSpec with Matchers {

  val wellFormedArguments: Seq[String] = Seq(
    "-s",
    getClass.getResource("/test_data_file.csv").getPath,
    "-j",
    getClass.getResource("/test_file.json").getPath,
    "-i",
    "thisIsAnId"
  )

  val wellFormedArgumentsWithDirectory: Seq[String] = Seq(
    "-s",
    "/tmp",
    "-j",
    getClass.getResource("/test_file.json").getPath
  )

  val badKeyFileArguments: Seq[String] = Seq(
    "-s",
    getClass.getResource("/test_data_file.csv").getPath,
    "-j",
    "non_existing_test_file.json"
  )

  "Config parser" should {
    "parse well formed arguments correctly" in {
      val actual: Option[ArgumentParser] =
        ArgumentParser.parser.parse(wellFormedArguments, ArgumentParser())
      assert(actual.isDefined)
    }

    "parse well formed arguments with directory correctly" in {
      val actual: Option[ArgumentParser] =
        ArgumentParser.parser.parse(wellFormedArgumentsWithDirectory, ArgumentParser())
      assert(actual.isDefined)
    }

    "fail when the JSON key file does not exist" in {
      val actual: Option[ArgumentParser] =
        ArgumentParser.parser.parse(badKeyFileArguments, ArgumentParser())
      assert(actual.isEmpty)
    }
  }



}
