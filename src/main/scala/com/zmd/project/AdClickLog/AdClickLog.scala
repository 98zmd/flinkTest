package com.zmd.project.AdClickLog

case class AdClickLog
(
  userId: Long,
  adId: Long,
  province: String,
  city: String,
  timestamp: Long
)

case class CountByProvince
(
  windowEnd: String,
  province: String,
  count: Long
)