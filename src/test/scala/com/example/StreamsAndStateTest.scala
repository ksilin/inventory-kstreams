package com.example

import io.confluent.common.utils.TestUtils
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest.freespec.AnyFreeSpecLike
import wvlet.log.LogSupport

import java.time.{Duration, Instant}
import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._

class StreamsAndStateTest extends AnyFreeSpecLike with LogSupport {

  val inputValues1: util.List[KeyValue[Integer, String]] = ((1 to 1000).toList map { i: Int => (new KeyValue[Integer, String](i, s"1_$i"))}).asJava
  val inputValues2: util.List[KeyValue[Integer, String]] = ((1 to 1000).toList map { i: Int => (new KeyValue[Integer, String](i, s"2_$i"))}).asJava

  val INPUT_TOPIC1 = "input1";
  val INPUT_TOPIC2 = "input2";

  val OUTPUT_TOPIC = "output";

  val streamsConfiguration: Properties = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-state-test")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath)
  val builder: StreamsBuilder = new StreamsBuilder()
  val consumed: Consumed[Integer, String] = Consumed.`with`(Serdes.Integer(), Serdes.String())

  "merged test" in {

    val countStoreSupplier: StoreBuilder[KeyValueStore[String, Integer]] = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("Counts"),
      Serdes.String(),
      Serdes.Integer())

   val topo: Topology = new Topology()
    topo.addSource("src", INPUT_TOPIC1)
    .addProcessor("counter", new CountProcessorSupplier(), "src")
      .addStateStore(countStoreSupplier, "counter")
      .addSink("sink", OUTPUT_TOPIC, new StringSerializer(),
        new IntegerSerializer(), "counter")

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(topo, streamsConfiguration)
    //
    // Step 2: Setup input and output topics.
    //
    val inputData1: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC1,
        new IntegerSerializer(),
        new StringSerializer())

    val output:  TestOutputTopic[String, Integer] = topologyTestDriver
      .createOutputTopic(OUTPUT_TOPIC,
        new StringDeserializer(),
       new IntegerDeserializer())

    inputData1.pipeKeyValueList(inputValues1, Instant.now(), Duration.ofSeconds(1));

    val outputValues: util.List[KeyValue[String, Integer]] = output.readKeyValuesToList()
    println("output:")
    outputValues.asScala foreach println
    // }

  }

}
