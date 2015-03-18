/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.BatchIterator
import com.treode.async.misc.RichOption
import com.treode.jackson.DefaultTreodeModule
import com.treode.store.{Bytes, TableId, TxClock}
import com.treode.twitter.finagle.http.{RichResponse, BadRequestException, RichRequest}
import com.twitter.finagle.http.{Request, Response, Status}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import com.twitter.finagle.http.filter.{CommonLogFormatter, LoggingFilter}
import com.twitter.logging.Logger
import com.twitter.finagle.server._
import com.twitter.finagle.{Http, Service}
import Http.Server
import com.twitter.finagle.{ServerCodecConfig, Stack}
import com.twitter.util._

import java.net.{InetSocketAddress, SocketAddress}
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.ListeningServer

import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.http.codec.{HttpClientDispatcher, HttpServerDispatcher}
import com.twitter.finagle.http.filter.DtabFilter
import com.twitter.finagle.http._
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.ssl.Ssl
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.tracing._
import java.net.{InetSocketAddress, SocketAddress}
import org.jboss.netty.channel.Channel
import org.jboss.netty.handler.codec.http._



package object example {

  implicit val textJson = new ObjectMapper with ScalaObjectMapper
  textJson.registerModule (DefaultScalaModule)
  textJson.registerModule (DefaultTreodeModule)
  textJson.registerModule (AppModule)

  val binaryJson = new ObjectMapper (new SmileFactory) with ScalaObjectMapper
  binaryJson.registerModule (DefaultScalaModule)

  object respond {

    def apply (req: Request, status: HttpResponseStatus = Status.Ok): Response = {
      val rsp = req.response
      rsp.status = status
      rsp
    }

    def clear (req: Request, status: HttpResponseStatus = Status.Ok): Response  = {
      val rsp = req.response
      rsp.status = status
      rsp.clearContent()
      rsp.contentLength = 0
      rsp
    }

    def json (req: Request, value: Any): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = value
      rsp
    }

    def json (req: Request, time: TxClock, value: Any): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.date = req.requestTxClock
      rsp.lastModified = time
      rsp.readTxClock = req.requestTxClock
      rsp.valueTxClock = time
      rsp.vary = "Request-TxClock"
      rsp.json = value
      rsp
    }

    def json [A] (req: Request, iter: BatchIterator [A]): Response  = {
      val rsp = req.response
      rsp.status = Status.Ok
      rsp.json = iter
      rsp
    }}

  object LoggingFilter extends LoggingFilter (Logger ("access"), new CommonLogFormatter)

  implicit class RichAny (v: Any) {

    def toJsonText: String =
      textJson.writeValueAsString (v)
  }

  implicit class RichBytes (bytes: Bytes) {

    def toJsonNode: JsonNode =
      binaryJson.readValue (bytes.bytes, classOf [JsonNode])
  }

  implicit class RichJsonNode (node: JsonNode) {

    def toBytes: Bytes =
      Bytes (binaryJson.writeValueAsBytes (node))
  }

  implicit class RichString (s: String) {

    def getTableId: TableId =
      TableId.parse (s) .getOrThrow (new BadRequestException (s"Bad table ID: $s"))

    def fromJson [A: Manifest]: A =
      textJson.readValue [A] (s)
  }

  object NewHttp extends com.twitter.finagle.Server[HttpRequest, HttpResponse] {

    import Http._

    class NewServer() extends Server {
      override protected def newDispatcher(transport: Transport[In, Out],
          service: Service[HttpRequest, HttpResponse]) = {
        val Stats(stats) = params[Stats]
          val dtab = DtabFilter.Netty
          new NewHttpServerDispatcher(
              new HttpTransport(transport), dtab andThen service, stats.scope("dispatch"))
      }
      protected def copy1(
          stack: Any,
          params: Any
          ): NewServer = this
    }

    val newServer = new NewServer();
    def serve(addr: SocketAddress, service: ServiceFactory[HttpRequest, HttpResponse]): ListeningServer =
      newServer.serve(addr, service)
  }
}
