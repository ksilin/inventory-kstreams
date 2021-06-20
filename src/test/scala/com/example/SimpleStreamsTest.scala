package com.example

import io.confluent.common.utils.TestUtils
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.freespec.AnyFreeSpecLike
import wvlet.log.{LogSupport, Logger}

import java.util
import java.util.{Arrays, List, Properties}
import scala.jdk.CollectionConverters._

class SimpleStreamsTest extends AnyFreeSpecLike with LogSupport {

  val inputValues: util.List[KeyValue[Integer, String]] = util.Arrays.asList(new KeyValue[Integer, String](1, "v1"), new KeyValue[Integer, String](1, "v1"), new KeyValue[Integer, String](1, "v2"))

  val INPUT_TOPIC = "input";
  val OUTPUT_TOPIC = "output";

  val streamsConfiguration: Properties = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath)
  val builder: StreamsBuilder = new StreamsBuilder()
  val consumed: Consumed[Integer, String] = Consumed.`with`(Serdes.Integer(), Serdes.String())
  val input: KStream[Integer, String] = builder.stream(INPUT_TOPIC, consumed)
    input.mapValues { v =>
      println(s"in stream: ${v}");
      v
    }.to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

  "init test" in {

    // val u: Try[Unit] = Using (new TopologyTestDriver(builder.build(), streamsConfiguration)) { topologyTestDriver: TopologyTestDriver =>

      val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
      //
      // Step 2: Setup input and output topics.
      //
      val inputData: TestInputTopic[Integer, String] = topologyTestDriver
        .createInputTopic(INPUT_TOPIC,
          new IntegerSerializer(),
          new StringSerializer())
      val output:  TestOutputTopic[Integer, String] = topologyTestDriver
        .createOutputTopic(OUTPUT_TOPIC,
          new IntegerDeserializer(),
          new StringDeserializer());
      //
      // Step 3: Produce some input data to the input topic.
      //
      inputData.pipeKeyValueList(inputValues);
      //
      // Step 4: Verify the application's output data.
      //
      val outputValues: util.List[KeyValue[Integer, String]] = output.readKeyValuesToList()
      println("output:")
      outputValues.asScala foreach println
    // }

  }

}
