package com.github.damdev.joiner

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.util.Collector

import scala.util.parsing.json.JSONObject





class CombineSameString extends RichCoFlatMapFunction[String, String, String] {
  private var one: ValueState[String] = _
  private var two: ValueState[String] = _

  override def open(parameters: Configuration): Unit = {
    one = getRuntimeContext.getState(new ValueStateDescriptor[String]("saved one", classOf[String]))
    two = getRuntimeContext.getState(new ValueStateDescriptor[String]("saved two", classOf[String]))
  }

  override def flatMap1(value: String, out: Collector[String]): Unit = {
    if (two.value() == null) {
      one.update(value)
    } else {
      out.collect(s"GET! 1:${value}|2:${two.value()}")
      two.clear()
    }
  }

  override def flatMap2(value: String, out: Collector[String]): Unit = {
    if (one.value() == null) {
      two.update(value)
    } else {
      out.collect(s"GET! 1:${one.value()}|2:${value}")
      one.clear()
    }
  }
}

object SocketTextStreamWordCount {

  def kafkaConsumer(topic: String, group: String): FlinkKafkaConsumer[ObjectNode] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", group)

    val consumer = new FlinkKafkaConsumer(topic, new JSONKeyValueDeserializationSchema(false), properties)

    consumer
  }

  def kStream(topic: String, group: String)(implicit env: StreamExecutionEnvironment): DataStream[ObjectNode] = env.addSource(kafkaConsumer(topic, group))

  def main(args: Array[String]): Unit = {
    implicit val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val stream1 = kStream("damtopic1", "damgroup1").map(_.get("value"))
    val stream2 = kStream("damtopic2", "damgroup2").map(_.get("value"))

    val counts1 = stream1.keyBy(s => s.get("id").asInt())
      .assignTimestampsAndWatermarks(new DamAssignerWithPeriodicWatermarks[JsonNode]())

    val counts2 = stream2.keyBy(s => s.get("id").asInt())
      .assignTimestampsAndWatermarks(new DamAssignerWithPeriodicWatermarks[JsonNode]())

    val joined: DataStream[String] = counts1
      .join(counts2)
      .where(s => s.get("id").asInt())
      .equalTo(s => s.get("id").asInt())
      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
//      .window(SlidingEventTimeWindows.of(Time.seconds(2) /* size */, Time.milliseconds(1) /* slide */))
      .apply { (a, b) =>
        val json = Map[String, Any](
        "id" -> a.get("id").asInt(),
        "a" -> (a.get("a").asText() + b.get("a").asText()))
        JSONObject(json).toString()
      }

    joined.addSink(
      new FlinkKafkaProducer[String]("localhost:9092", "damresult", new SimpleStringSchema())
    ).name("damresult-kafka")

    env.execute("Scala SocketTextStreamWordCount Example")
  }

}
