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

package com.treode.disk

import java.nio.file.Paths
import java.util.logging.{Level, Logger}

import com.treode.async.Async
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.implicits._
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import DiskTestTools._

class DiskDriveSpec extends FreeSpec {

  Logger.getLogger ("com.treode") .setLevel (Level.WARNING)

  class DistinguishedException extends Exception

  implicit val config = DiskTestConfig()

  private def init (file: File, kit: DiskKit) = {
    val path = Paths.get ("a")
    val free = IntSet()
    val boot = BootBlock (sysid, 0, 0, Set (path))
    val geom = DriveGeometry.test()
    new SuperBlock (0, boot, geom, false, free, 0)
    DiskDrive.init (0, path, file, geom, boot, kit)
  }

  "DiskDrive.init should" - {

    "work when all is well" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, 6)
      val kit = new DiskKit (sysid, 0)
      init (file, kit) .expectPass()
    }

    "issue three writes to the disk" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, 6)
      val kit = new DiskKit (sysid, 0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.run()
      file.last.pass (())
      file.last.pass (())
      file.last.pass (())
      file.stop = false
      scheduler.run()
      cb.assertPassed()
    }

    "fail when it cannot write the superblock" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, 6)
      val kit = new DiskKit (sysid, 0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.run()
      file.last.pass (())
      file.last.fail (new DistinguishedException)
      file.last.pass (())
      file.stop = false
      scheduler.run()
      cb.assertFailed [DistinguishedException]
    }

    "fail when it cannot clear the next superblock" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, 6)
      val kit = new DiskKit (sysid, 0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.run()
      file.last.pass (())
      file.last.pass (())
      file.last.fail (new DistinguishedException)
      file.stop = false
      scheduler.run()
      cb.assertFailed [DistinguishedException]
    }

    "fail when it cannot write the log tail" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, 6)
      val kit = new DiskKit (sysid, 0)
      file.stop = true
      val cb = init (file, kit) .capture()
      scheduler.run()
      file.last.fail (new DistinguishedException)
      file.last.pass (())
      file.last.pass (())
      file.stop = false
      scheduler.run()
      cb.assertFailed [DistinguishedException]
    }}}
