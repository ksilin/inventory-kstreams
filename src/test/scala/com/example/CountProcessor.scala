package com.example

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, ProcessorSupplier, Record}
import org.apache.kafka.streams.processor.{PunctuationType, Punctuator}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}

import java.time.Duration

class CountProcessor extends Processor[Integer, String, String, Integer]{

  var ctx: ProcessorContext[String, Integer] = _
  var kvStore: KeyValueStore[String, Integer] = _

  override def init(context: ProcessorContext[String, Integer]): Unit = {
    println(s"received ctx: $context")
    this.ctx = context
    this.kvStore = context.getStateStore("Counts")

    ctx.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, new Punctuator() {
      override def punctuate(timestamp: Long): Unit = {
        // forward all
        val iter: KeyValueIterator[String, Integer] = kvStore.all()
        while (iter.hasNext) {
          val entry: KeyValue[String, Integer] = iter.next();
          if(entry.value > 15) {
            val record: Record[String, Integer] = new Record[String, Integer](entry.key, entry.value, timestamp)
            println(s"forwarding ${entry.key} : ${entry.value}")
            context.forward(record)
            kvStore.put(entry.key, 0)
          }
        }
        iter.close();
        // commit the current processing progress
        context.commit();
      }
    })
  }

  override def process(record: Record[Integer, String]): Unit = {

    val meta = ctx.recordMetadata().get()
    val topic = meta.topic()

    println(s"processing $topic:${meta.partition}:${meta.offset}: ${record.key} : ${record.value}")

    val countBefore: Int = kvStore.get(topic)
    val currentCount = countBefore + 1
    kvStore.put(topic, currentCount)
    println(s"count for $topic : $currentCount")
  }

  override def close(): Unit = {
    println("nothing to do in close")
  }
}

class CountProcessorSupplier extends ProcessorSupplier[Integer, String, String, Integer]{

  def get() = new CountProcessor()
}