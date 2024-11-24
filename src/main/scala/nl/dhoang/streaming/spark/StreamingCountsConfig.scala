package nl.dhoang.streaming.spark

case class StreamingCountsConfig(
                                  region:             String,
                                  streamName:         String,
                                  checkpointInterval: Duration,
                                  initialPosition:    InitialPositionInStream,
                                  storageLevel:       StorageLevel,
                                  appName:            String,
                                  master:             String,
                                  batchInterval:      Duration,
                                  tableName:          String,
                                  awsProfile:         String
                                ) {

  /**
   * The Kinesis endpoint from the region.
   */
  val endpointUrl = s"https://kinesis.${region}.amazonaws.com"
}