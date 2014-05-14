package com.treode.disk.stubs

import scala.collection.mutable.UnrolledBuffer
import scala.util.{Failure, Random, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.disk._

import Async.{async, guard, supply}
import Callback.fanout
import Disks.{Launch, Recovery}

private class StubDisks (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubConfig
) extends Disks {

  val logd = new Dispatcher [(StubRecord, Callback [Unit])] (0L)
  val checkpointer = new StubCheckpointer
  val releaser = new Releaser

  logd.receive (receiver _)

  private def align (n: Int): Int = {
    val bits = 6
    val mask = (1 << bits) - 1
    (n + mask) & ~mask
  }

  private def receiver (batch: Long, records: UnrolledBuffer [(StubRecord, Callback [Unit])]) {
    logd.replace (new UnrolledBuffer)
    val cb = fanout (records .map (_._2))
    disk.log (records .map (_._1) .toSeq) .run { v =>
      logd.receive (receiver _)
      cb (v)
    }}

  def launch (checkpoints: CheckpointRegistry) {
    checkpointer.launch (checkpoints)
  }

  def record [R] (desc: RecordDescriptor [R], entry: R): Async [Unit] =
    async { cb =>
      logd.send ((StubRecord (desc, entry), cb))
      checkpointer.tally()
    }

  def read [P] (desc: PageDescriptor [_, P], pos: Position): Async [P] =
    guard {
      for {
        page <- disk.read (pos.offset)
      } yield {
        require (align (page.length) == pos.length)
        desc.ppag.fromByteArray (page.data)
      }}

  def write [G, P] (desc: PageDescriptor [G, P], obj: ObjectId, group: G, page: P): Async [Position] =
    guard {
      val _page = StubPage (desc, obj, group, page)
      for {
        offset <- disk.write (_page)
      } yield {
        Position (0, offset, align (_page.length))
      }}

  def compact (desc: PageDescriptor [_, _], obj: ObjectId): Async [Unit] =
    supply()

  def join [A] (task: Async [A]): Async [A] =
    releaser.join (task)
}

object StubDisks {

  trait StubRecovery extends Recovery {

    def reattach (disk: StubDiskDrive): Async [Launch]

    def attach (disk: StubDiskDrive): Async [Launch]
  }

  def recover (
      checkpoint: Double = 0.1
  ) (implicit
      random: Random,
      scheduler: Scheduler
  ): StubRecovery = {
    implicit val config = StubConfig (checkpoint)
    new StubRecoveryAgent
  }}