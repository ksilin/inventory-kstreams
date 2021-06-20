package com.example

import io.confluent.common.utils.TestUtils
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Consumed, JoinWindows, Joined, KStream, Produced, StreamJoined, ValueJoiner}
import org.apache.kafka.streams._
import org.scalatest.freespec.AnyFreeSpecLike
import wvlet.log.LogSupport

import java.time.Duration
import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._

class StreamsMergeTest extends AnyFreeSpecLike with LogSupport {




  val inputValues1: util.List[KeyValue[Integer, String]] = ((1 to 1000).toList map { i: Int => (new KeyValue[Integer, String](i, s"1_$i"))}).asJava
  val inputValues2: util.List[KeyValue[Integer, String]] = ((1 to 1000).toList map { i: Int => (new KeyValue[Integer, String](i, s"2_$i"))}).asJava

  val INPUT_TOPIC1 = "input1";
  val INPUT_TOPIC2 = "input2";

  val OUTPUT_TOPIC = "output";

  val streamsConfiguration: Properties = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "merge-test")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  // Use a temporary directory for storing state, which will be automatically removed after the test.
  streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath)
  val builder: StreamsBuilder = new StreamsBuilder()
  val consumed: Consumed[Integer, String] = Consumed.`with`(Serdes.Integer(), Serdes.String())

  "multi-input test" in {

    val input: KStream[Integer, String] = builder.stream(List(INPUT_TOPIC1, INPUT_TOPIC2).asJava, consumed)
    input.mapValues { v =>
      println(s"in stream: ${v}");
      v
    }.to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

    // val u: Try[Unit] = Using (new TopologyTestDriver(builder.build(), streamsConfiguration)) { topologyTestDriver: TopologyTestDriver =>

      val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
      //
      // Step 2: Setup input and output topics.
      //
      val inputData1: TestInputTopic[Integer, String] = topologyTestDriver
        .createInputTopic(INPUT_TOPIC1,
          new IntegerSerializer(),
          new StringSerializer())
    val inputData2: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC2,
        new IntegerSerializer(),
        new StringSerializer())
      val output:  TestOutputTopic[Integer, String] = topologyTestDriver
        .createOutputTopic(OUTPUT_TOPIC,
          new IntegerDeserializer(),
          new StringDeserializer());
      //
      // Step 3: Produce some input data to the input topic.
      //
      inputData1.pipeKeyValueList(inputValues1);
      inputData2.pipeKeyValueList(inputValues2);
      //
      // Step 4: Verify the application's output data.
      //
      val outputValues: util.List[KeyValue[Integer, String]] = output.readKeyValuesToList()
      println("output:")
      outputValues.asScala foreach println
    // }

  }

  "unmerged test" in {

    val input1: KStream[Integer, String] = builder.stream(INPUT_TOPIC1, consumed)
    input1.mapValues { v =>
      println(s"in stream 1: ${v}");
      v
    }.to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

    val input2: KStream[Integer, String] = builder.stream(INPUT_TOPIC2, consumed)
    input2.mapValues { v =>
      println(s"in stream 2: ${v}");
      v
    }.to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

    // val u: Try[Unit] = Using (new TopologyTestDriver(builder.build(), streamsConfiguration)) { topologyTestDriver: TopologyTestDriver =>

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    //
    // Step 2: Setup input and output topics.
    //
    val inputData1: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC1,
        new IntegerSerializer(),
        new StringSerializer())
    val inputData2: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC2,
        new IntegerSerializer(),
        new StringSerializer())

    val output:  TestOutputTopic[Integer, String] = topologyTestDriver
      .createOutputTopic(OUTPUT_TOPIC,
        new IntegerDeserializer(),
        new StringDeserializer());
    //
    // Step 3: Produce some input data to the input topic.
    //
    inputData1.pipeKeyValueList(inputValues1);
    inputData2.pipeKeyValueList(inputValues2);
    //
    // Step 4: Verify the application's output data.
    //
    val outputValues: util.List[KeyValue[Integer, String]] = output.readKeyValuesToList()
    println("output:")
    outputValues.asScala foreach println
    // }
  }

  "merged test" in {

    val input1: KStream[Integer, String] = builder.stream(INPUT_TOPIC1, consumed)
    input1.mapValues { v =>
      println(s"in stream 1: ${v}");
      v
    }

    val input2: KStream[Integer, String] = builder.stream(INPUT_TOPIC2, consumed)
    input2.mapValues { v =>
      println(s"in stream 2: ${v}");
      v
    }

    input1.merge(input2).to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

    // val u: Try[Unit] = Using (new TopologyTestDriver(builder.build(), streamsConfiguration)) { topologyTestDriver: TopologyTestDriver =>

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    //
    // Step 2: Setup input and output topics.
    //
    val inputData1: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC1,
        new IntegerSerializer(),
        new StringSerializer())
    val inputData2: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC2,
        new IntegerSerializer(),
        new StringSerializer())

    val output:  TestOutputTopic[Integer, String] = topologyTestDriver
      .createOutputTopic(OUTPUT_TOPIC,
        new IntegerDeserializer(),
        new StringDeserializer());
    //
    // Step 3: Produce some input data to the input topic.
    //
    inputData1.pipeKeyValueList(inputValues1);
    inputData2.pipeKeyValueList(inputValues2);
    //
    // Step 4: Verify the application's output data.
    //
    val outputValues: util.List[KeyValue[Integer, String]] = output.readKeyValuesToList()
    println("output:")
    outputValues.asScala foreach println
    // }
  }

  "joined test" in {

    val input1: KStream[Integer, String] = builder.stream(INPUT_TOPIC1, consumed)
    input1.mapValues { v =>
      println(s"in stream 1: ${v}");
      v
    }

    val input2: KStream[Integer, String] = builder.stream(INPUT_TOPIC2, consumed)
    input2.mapValues { v =>
      println(s"in stream 2: ${v}");
      v
    }

    val joiner: ValueJoiner[String, String, String] = (l: String, r: String) => s"$l+$r"

    input1.join(input2, joiner, JoinWindows.of(Duration.ofSeconds(2)), StreamJoined.`with`(Serdes.Integer(), Serdes.String, Serdes.String))
      .to(OUTPUT_TOPIC, Produced.`with`(Serdes.Integer(), Serdes.String))

    // val u: Try[Unit] = Using (new TopologyTestDriver(builder.build(), streamsConfiguration)) { topologyTestDriver: TopologyTestDriver =>

    val topologyTestDriver: TopologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)
    //
    // Step 2: Setup input and output topics.
    //
    val inputData1: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC1,
        new IntegerSerializer(),
        new StringSerializer())
    val inputData2: TestInputTopic[Integer, String] = topologyTestDriver
      .createInputTopic(INPUT_TOPIC2,
        new IntegerSerializer(),
        new StringSerializer())

    val output:  TestOutputTopic[Integer, String] = topologyTestDriver
      .createOutputTopic(OUTPUT_TOPIC,
        new IntegerDeserializer(),
        new StringDeserializer());
    //
    // Step 3: Produce some input data to the input topic.
    //
    inputData1.pipeKeyValueList(inputValues1);
    inputData2.pipeKeyValueList(inputValues2);
    //
    // Step 4: Verify the application's output data.
    //
    val outputValues: util.List[KeyValue[Integer, String]] = output.readKeyValuesToList()
    println("output:")
    outputValues.asScala foreach println
    // }
  }

}
