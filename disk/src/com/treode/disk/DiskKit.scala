package com.treode.disk

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.EpochReleaser

import Async.{guard, latch}
import Callback.ignore

private class DiskKit (
    logBatch: Long
) (implicit
    val scheduler: Scheduler,
    val config: DiskConfig
) {

  val logd = new Dispatcher [PickledRecord] (logBatch)
  val paged = new Dispatcher [PickledPage] (0L)
  val disks = new DiskDrives (this)
  val checkpointer = new Checkpointer (this)
  val releaser = new EpochReleaser
  val compactor = new Compactor (this)

  def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
    guard {
      for {
        _ <- latch (
            checkpointer.launch (checkpoints),
            compactor.launch (pages))
        _ <- disks.launch()
      } yield ()
    } run (ignore)
}