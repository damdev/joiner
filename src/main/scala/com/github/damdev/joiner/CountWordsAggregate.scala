package com.github.damdev.joiner

import org.apache.flink.api.common.functions.AggregateFunction

class CountWordsAggregate extends AggregateFunction[String, Map[String, Int], Map[String, Int]] {
  override def createAccumulator(): Map[String, Int] = Map[String, Int]()

  override def add(value: String, accumulator: Map[String, Int]) = {
    accumulator.updated(value, accumulator.getOrElse(value, 0) + 1)
  }

  override def getResult(accumulator: Map[String, Int]) = accumulator

  override def merge(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] = {
    (a.toSeq ++ b.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum)
  }
}
