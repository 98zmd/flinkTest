package com.zmd.project.LogFail

case class LoginEvent
(
  userId: Long,
  ip: String,
  eventType: String,
  eventTime: Long
)
