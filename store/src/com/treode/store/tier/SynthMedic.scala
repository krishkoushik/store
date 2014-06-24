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

package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.Scheduler
import com.treode.disk.{Disk, Position}
import com.treode.store.{Bytes, Cell, Key, Store, TableId, TxClock}

import SynthTable.{genStepBits, genStepMask, genStepSize}

private class SynthMedic (
    desc: TierDescriptor,
    id: TableId
) (implicit
    scheduler: Scheduler,
    config: Store.Config
) extends TierMedic {

  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var gen = genStepSize

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTier

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTier

  // The position of each tier on disk.
  private var tiers = Tiers.empty

  private def replay (gen: Long, key: Bytes, time: TxClock, value: Option [Bytes]) {

    val mkey = Key (key, time)

    readLock.lock()
    val needWrite = try {
      if (gen < this.gen) {
        secondary.put (mkey, value)
        false
      } else if (gen == this.gen) {
        primary.put (mkey, value)
        false
      } else if (gen > this.gen) {
        true
      } else {
        false
      }
    } finally {
      readLock.unlock()
    }

    if (needWrite) {
      writeLock.lock()
      try {
        if (gen < this.gen) {
          secondary.put (mkey, value)
        } else if (gen == this.gen) {
          primary.put (mkey, value)
        } else if (gen > this.gen) {
          this.gen = gen
          secondary.putAll (primary)
          primary = newMemTier
          primary.put (mkey, value)
        }
      } finally {
        writeLock.unlock()
      }}}

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes): Unit =
    replay (gen, key, time, Some (value))

  def delete (gen: Long, key: Bytes, time: TxClock): Unit =
    replay (gen, key, time, None)

  private def putAll (tier: MemTier, cells: Seq [Cell]): Unit =
    for (c <- cells)
      tier.put (c.timedKey, c.value)

  def receive (gen: Long, novel: Seq [Cell]) {

    writeLock.lock()
    try {
      if (gen < this.gen) {
        putAll (secondary, novel)
      } else if (gen == this.gen) {
        putAll (primary, novel)
      } else if (gen > this.gen) {
        this.gen = gen
        secondary.putAll (primary)
        primary = newMemTier
        putAll (primary, novel)
      }
    } finally {
      writeLock.unlock()
    }}

  def checkpoint (meta: TierTable.Meta) {
    writeLock.lock()
    val mg = meta.gen >> genStepBits
    val tg = this.gen >> genStepBits
    try {
      if (mg == tg - 1) {
        secondary = newMemTier
      } else if (mg >= tg) {
        gen = (mg+1) << genStepBits
        primary = newMemTier
        secondary = newMemTier
      }
      if (meta.gen > tiers.gen || meta.tiers > tiers)
        tiers = meta.tiers
    } finally {
      writeLock.unlock()
    }}

  def close () (implicit launch: Disk.Launch): TierTable = {
    import launch.disk

    writeLock.lock()
    val (gen, primary, tiers) = try {
      this.secondary.putAll (this.primary)
      val result = (this.gen, this.secondary, this.tiers)
      this.primary = null
      this.secondary = null
      this.tiers = null
      result
    } finally {
      writeLock.unlock()
    }

    new SynthTable (desc, id, lock, gen, primary, newMemTier, tiers)
  }}
