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

//import com.treode.store.{Bytes, TableId, Cell, Bound, TxClock, Batch, Window, Slice, WriteOp, StaleException, Key, TxId, Store, ReadOp, Value}, stubs.StubStore
import com.treode.store._, WriteOp._
import com.treode.store.stubs.StubStore

import com.treode.async.Async, Async.supply
import com.treode.async.BatchIterator

import com.treode.twitter.app.StoreKit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.treode.async.misc.RichOption
import com.treode.jackson.DefaultTreodeModule
import com.treode.twitter.finagle.http.{RichResponse, BadRequestException, RichRequest}
import com.twitter.finagle.http.{Request, Response, Status}
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import com.twitter.finagle.http.filter.{CommonLogFormatter, LoggingFilter}
import com.twitter.logging.Logger
import scala.collection.mutable.HashMap



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
  
  class Schema (mapping: HashMap [String, Long]) {
    
	def getTableId (s: String): TableId = {
      val id = mapping.get (s)
      id match {
        case Some (id)
          => id. toString(). getTableId
		case None =>
          throw new BadRequestException (s"Bad table ID: $s")
	  }}
  }

  class SchematicStore (store: Store, schema: Schema) {

    def read (name: String, key: String, rt: TxClock): Async [Seq [Value]]
      
    def update (name: String, key: String, value: JsonNode, tx: TxId, ct: TxClock): Async [TxClock]
  
    def delete (name: String, key: String, tx: TxId, ct: TxClock): Async [TxClock]

    def scan (
      name: String,
      key: Bound [Key] = Bound.firstKey,
      window: Window = Window.all,
      slice: Slice = Slice.all,
      batch: Batch = Batch.suggested
    ): BatchIterator [Cell]
  }

  class SchematicStubStore (store: StubStore, schema: Schema) extends SchematicStore (store, schema) {

    def read (name: String, key: String, rt: TxClock): Async [Seq [Value]] = {
      val ops = Seq (ReadOp (schema.getTableId(name), Bytes (key)))
      store.read (rt, ops:_*)
    }

    def update (name: String, key: String, value: JsonNode, tx: TxId, ct: TxClock): Async [TxClock] = {
      val ops = Seq (WriteOp.Update (schema.getTableId(name), Bytes (key), value.toBytes))
      store.write (tx, ct, ops:_*)
    }

    def delete (name: String, key: String, tx: TxId, ct: TxClock): Async [TxClock] = {
      val ops = Seq (WriteOp.Delete (schema.getTableId(name), Bytes (key)))
      store.write (tx, ct, ops:_*)
    }

    def scan (
      name: String,
      key: Bound [Key],
      window: Window,
      slice: Slice,
      batch: Batch
    ): BatchIterator [Cell] = {
      store.scan (schema.getTableId (name), key, window, slice, batch)
    }

    def scan (name: String): Seq[Cell] = {
      store.scan(schema.getTableId (name))
    }
  }
}
