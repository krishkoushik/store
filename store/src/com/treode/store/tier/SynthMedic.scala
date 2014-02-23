package com.treode.store.tier

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.treode.async.Scheduler
import com.treode.disk.{Disks, Position, TypeId}
import com.treode.store.{Bytes, StoreConfig}

private class SynthMedic [K, V] (
    desc: TierDescriptor [K, V]
) (implicit
    scheduler: Scheduler,
    config: StoreConfig
) extends TierMedic {

  private val lock = new ReentrantReadWriteLock
  private val readLock = lock.readLock()
  private val writeLock = lock.writeLock()

  private var generation = 0L

  // This resides in memory and it is the only tier that is written.
  private var primary = newMemTier

  // This tier resides in memory and is being compacted and written to disk.
  private var secondary = newMemTier

  // The position of each tier on disk.
  private var tiers = Tiers.empty

  private def replay (gen: Long, key: Bytes, value: Option [Bytes]) {

    readLock.lock()
    val needWrite = try {
      if (gen == this.generation - 1) {
        secondary.put (key, value)
        false
      } else if (gen == this.generation) {
        primary.put (key, value)
        false
      } else {
        true
      }
    } finally {
      readLock.unlock()
    }

    if (needWrite) {
      writeLock.lock()
      try {
        if (gen == this.generation - 1) {
          secondary.put (key, value)
        } else if (gen == this.generation) {
          primary.put (key, value)
        } else if (gen == this.generation + 1) {
          this.generation = gen
          secondary = primary
          primary = newMemTier
          primary.put (key, value)
        } else if (gen > this.generation + 1) {
          this.generation = gen
          primary = newMemTier
          secondary = newMemTier
          primary.put (key, value)
        }
      } finally {
        writeLock.unlock()
      }}}

  def put (gen: Long, key: Bytes, value: Bytes): Unit =
    replay (gen, key, Some (value))

  def delete (gen: Long, key: Bytes): Unit =
    replay (gen, key, None)

  def checkpoint (meta: TierTable.Meta) {
    writeLock.lock()
    try {
      generation = meta.gen+1
      primary = newMemTier
      secondary = newMemTier
      tiers = meta.tiers
    } finally {
      writeLock.unlock()
    }}

  def close () (implicit launch: Disks.Launch): TierTable = {
    import launch.disks

    writeLock.lock()
    val (generation, primary, secondary, tiers) = try {
      if (!this.secondary.isEmpty) {
        this.secondary.putAll (this.primary)
        this.primary = this.secondary
        this.secondary = newMemTier
      }
      val result = (this.generation, this.primary, this.secondary, this.tiers)
      this.primary = null
      this.secondary = null
      this.tiers = null
      result
    } finally {
      writeLock.unlock()
    }

    val table = new SynthTable (desc, lock, generation, primary, secondary, tiers)
    desc.pager.handle (table)
    table
  }}
