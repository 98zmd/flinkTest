package com.zmd.api

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class SelfWatermark extends AssignerWithPeriodicWatermarks[sendor]{

  override def getCurrentWatermark: Watermark = ??? //200毫秒生成一次

  override def extractTimestamp(t: sendor, l: Long): Long = ??? //
}
