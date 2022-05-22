package com.zmd.realtimeTrafficAnalysis

case class ApacheLogEvent
(
  ip: String,
  userId: String,
  eventTime: Long,
  method: String,
  url: String
)
