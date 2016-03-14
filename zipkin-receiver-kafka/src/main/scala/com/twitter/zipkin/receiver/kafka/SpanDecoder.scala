package com.twitter.zipkin.receiver.kafka

import com.twitter.scrooge.TJSONProtocolThriftSerializer
import com.twitter.zipkin.thriftscala.{Span => ThriftSpan}
import com.twitter.logging.Logger


class SpanDecoder extends KafkaProcessor.KafkaDecoder {

  private[this] val deserializer = new TJSONProtocolThriftSerializer[ThriftSpan] {
    val codec = ThriftSpan
  }
  private[this] val log = Logger.get(getClass.getName)


  override def fromBytes(bytes: Array[Byte]): Option[List[ThriftSpan]] = {
    try {
      Some(List {
        deserializer.fromBytes(bytes)
      })
    }
    catch {
      case e: Throwable => {
        log.error(s"${e.getCause}")
        None
      }
    }
  }
}
