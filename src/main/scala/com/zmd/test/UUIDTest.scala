package com.zmd.test

import java.util.UUID

object UUIDTest {
  def main(args: Array[String]): Unit = {
    val uuid: UUID = UUID.randomUUID()
    println(uuid.getMostSignificantBits)
    println(uuid.getLeastSignificantBits)
  }
}
