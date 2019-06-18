package com.github.damdev.joiner

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class DamAssignerWithPeriodicWatermarks[T] extends AssignerWithPeriodicWatermarks[T] {
  override def getCurrentWatermark: Watermark = new Watermark(System.currentTimeMillis())

  override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = System.currentTimeMillis()
}
