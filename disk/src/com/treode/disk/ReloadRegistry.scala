package com.treode.disk

import com.treode.async.Async
import com.treode.pickle.PicklerRegistry

import PicklerRegistry.FunctionTag

class ReloadRegistry {

  val loaders =
    PicklerRegistry [FunctionTag [Disks.Reload, Async [Unit]]] ("ReloadRegistry")

  def reload [B] (desc: RootDescriptor [B]) (f: B => Disks.Reload => Async [Unit]): Unit =
    PicklerRegistry.curried (loaders, desc.pblk, desc.id.id) (f)

  def pager = CheckpointRegistry.pager (loaders.pickler)
}
