package com.zmd.api

import org.apache.flink.api.common.functions.ReduceFunction

case class MyReducer() extends ReduceFunction[sendor]{
  override def reduce(t: sendor, t1: sendor): sendor = {
    t
  }
}
