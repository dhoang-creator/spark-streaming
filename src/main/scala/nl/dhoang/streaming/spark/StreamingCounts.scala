package nl.dhoang.streaming.spark

object StreamingCounts {

  private def setupSparkContext(config: StreamingCountsConfig): StreamingContext = {
    val streamingSparkContext = {
      val sparkConf = new SparkConf().setAppName(config.appName).setMaster(config.master)
      new StreamingContext(sparkConf, config.batchInterval)
    }
    streamingSparkContext
  }

  def execute(config: StreamingCountsConfig) {

    // setting up Spark Streaming connection to Kinesis
    val kinesisClient = KU.setupKinesisClientConnection(config.endpointUrl, config.awsProfile)
    require(kinesisClient != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")

    // setting up Spark Streaming connection to DynamoDB
    lazy val dynamoConnection = DynamoUtils.setupDynamoClientConnection(config.awsProfile)

    val streamingSparkContext = setupSparkContext(config)
    val numShards = KU.getShardCount(kinesisClient, config.streamName)
    val sparkDStreams = (0 until numShards).map { i =>
      KinesisUtils.createStream(
        ssc = streamingSparkContext,
        streamName = config.streamName,
        endpointUrl = config.endpointUrl,
        initialPositionInStream = config.initialPosition,
        checkpointInterval = config.batchInterval,
        storageLevel = config.storageLevel
      )
    }

    // Map phase: union DStreams, derive events, determine bucket
    val bucketedEvents = streamingSparkContext
      .union(sparkDStreams)
      .map { bytes =>
        val e = SimpleEvent.fromJson(bytes)
        (e.bucket, e.`type`)
      }

    // Reduce phase: group by key then by count
    val bucketedEventCounts = bucketedEvents
      .groupByKey
      .map { case (eventType, events) =>
        val count = events.groupBy(identity).mapValues(_.size)
        (eventType, count)
      }

    // Iterate over each aggregate record and save the record into DynamoDB
    bucketedEventCounts.foreachRDD { rdd =>
      rdd.foreach { case (bucket, aggregates) =>
        aggregates.foreach { case (eventType, count) =>
          DynamoUtils.setOrUpdateCount(
            dynamoConnection,
            config.tableName,
            bucket.toString,
            eventType,
            DynamoUtils.timeNow(),
            DynamoUtils.timeNow(),
            count.toInt
          )
        }
      }
    }

    // Start Spark Streaming process
    streamingSparkContext.start()
    streamingSparkContext.awaitTermination()
  }

}
