package nl.dhoang.streaming.spark

import org.apache.spark.storage.StorageLevel

import java.io.File

object StreamingCountsApp {

  def main(args: Array[String]) {

    // General bumf for our app
    val parser = new ArgotParser(
      programName = "generated",
      compactUsage = true,
      preUsage = Some("%s: Version %s. Copyright (c) 2015, %s.".format(
        generated.Settings.name,
        generated.Settings.version,
        generated.Settings.organization)
      )
    )

    // Optional config argument
    val config = parser.option[Config](List("config"),
      "filename",
      "Configuration file.") {
      (c, opt) =>

        val file = new File(c)
        if (file.exists) {
          ConfigFactory.parseFile(file)
        } else {
          parser.usage("Configuration file \"%s\" does not exist".format(c))
          ConfigFactory.empty()
        }
    }
    parser.parse(args)

    // read the config file if --config parameter is provided else fail
    val conf = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))

    // create Spark Streaming Config from hocon file in resource directory
    val scc = StreamingCountsConfig(
      region = conf.getConfig("kinesis").getString("region"),
      streamName = conf.getConfig("kinesis").getString("streamName"),
      checkpointInterval = Minutes(conf.getConfig("spark").getInt("checkpointInterval")),
      initialPosition = InitialPositionInStream.LATEST,
      storageLevel = StorageLevel.MEMORY_AND_DISK_2,
      appName = conf.getConfig("spark").getString("appName"),
      master = conf.getConfig("spark").getString("master"),
      batchInterval = Milliseconds(conf.getConfig("spark").getInt("batchInterval")),
      tableName = conf.getConfig("dynamodb").getString("tableName"),
      awsProfile = conf.getConfig("aws").getString("awsProfile")
    )

    // start StreamingCounts application with config object
    StreamingCounts.execute(scc)
  }

}
