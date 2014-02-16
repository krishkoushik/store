package com.treode.store.tier

import java.nio.file.Paths
import scala.collection.mutable.Builder

import com.treode.async.{Async, Callback, RichIterable, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import com.treode.store._
import com.treode.disk.{Disks, DisksConfig, DiskGeometry, PageDescriptor, Position}
import org.scalatest.WordSpec

import Async.async
import Cardinals.One
import Fruits.{AllFruits, Apple, Orange, Watermelon}
import TestTable.descriptor
import TierTestTools._

class TierSpec extends WordSpec {

  private def setup() = {
    implicit val scheduler = StubScheduler.random()
    implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val file = new StubFile
    val geometry = DiskGeometry (20, 12, 1<<30)
    val item = (Paths.get ("a"), file, geometry)
    val disks = recovery.attach (Seq (item)) .pass
    (scheduler, disks)
  }

  /** Get the depths of ValueBlocks reached from the index entries. */
  private def getDepths (entries: Iterable [IndexEntry], depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] =
    entries.map (e => getDepths (e.pos, depth+1)) .fold (Set.empty) (_ ++ _)

  /** Get the depths of ValueBlocks for the tree root at `pos`. */
  private def getDepths (pos: Position, depth: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Set [Int] = {
    descriptor.pager.read (pos) .pass match {
      case b: IndexPage => getDepths (b.entries, depth+1)
      case b: CellPage => Set (depth)
    }}

  /** Check that tree rooted at `pos` has all ValueBlocks at the same depth, expect those under
    * the final index entry.
    */
  private def expectBalanced (tier: Tier) (implicit scheduler: StubScheduler, disks: Disks) {
    descriptor.pager.read (tier.root) .pass match {
      case b: IndexPage =>
        val ds1 = getDepths (b.entries.take (b.size-1), 1)
        expectResult (1, "Expected lead ValueBlocks at the same depth.") (ds1.size)
        val d = ds1.head
        val ds2 = getDepths (b.last.pos, 1)
        expectResult (true, "Expected final ValueBlocks at depth < $d") (ds2 forall (_ < d))
      case b: CellPage =>
        ()
    }}

  /** Build a tier from fruit. */
  private def buildTier (pageBytes: Int) (
      implicit scheduler: StubScheduler, disks: Disks): Tier = {
    implicit val config = StoreConfig (pageBytes)
    val builder = new TierBuilder (descriptor, 0)
    AllFruits.async.foreach (builder.add (_, Some (One))) .pass
    builder.result.pass
  }

  /** Build a sequence of the cells in the tier by using the TierIterator. */
  private def iterateTier (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [Cell] =
    TierIterator (descriptor, tier.root) .toSeq

  private def toSeq (builder: Builder [Cell, _], pos: Position) (
      implicit scheduler: StubScheduler, disks: Disks) {
    descriptor.pager.read (pos) .pass match {
      case page: IndexPage =>
        page.entries foreach (e => toSeq (builder, e.pos))
      case page: CellPage =>
        page.entries foreach (builder += _)
    }}

  /** Build a sequence of the cells in the tier using old-fashioned recursion. */
  private def toSeq (tier: Tier) (
      implicit scheduler: StubScheduler, disks: Disks): Seq [Cell] = {
    val builder = Seq.newBuilder [Cell]
    toSeq (builder, tier.root)
    builder.result
  }

  "The TierBuilder" should {

    "require that added entries are not duplicated" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (descriptor, 0)
      builder.add (Apple, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "require that added entries are sorted by key" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (descriptor, 0)
      builder.add (Orange, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "require that added entries are reverse sorted by time" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (descriptor, 0)
      builder.add (Apple, None) .pass
      builder.add (Apple, None) .fail [IllegalArgumentException]
    }

    "allow properly sorted entries" in {
      implicit val (scheduler, disks) = setup()
      implicit val config = StoreConfig (1 << 16)
      val builder = new TierBuilder (descriptor, 0)
      builder.add (Apple, None) .pass
      builder.add (Orange, None) .pass
      builder.add (Watermelon, None) .pass
    }

    "build a blanced tree with all keys" when {

      def checkBuild (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectBalanced (tier)
        expectResult (AllFruits.toSeq) (toSeq (tier) .map (_.key))
      }

      "the pages are limited to one byte" in {
        checkBuild (1)
      }

      "the pages are limited to 256 bytes" in {
        checkBuild (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkBuild (1 << 16)
      }}}

  "The TierIterator" should {

    "iterate all keys" when {

      def checkIterator (pageBytes: Int) {
        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)
        expectResult (AllFruits.toSeq) (iterateTier (tier) map (_.key))
      }

      "the pages are limited to one byte" in {
        checkIterator (1)
      }

      "the pages are limited to 256 bytes" in {
        checkIterator (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkIterator (1 << 16)
      }}}

  "The Tier" should {

    "find the key" when {

      def checkFind (pageBytes: Int) {

        implicit val (scheduler, disks) = setup()
        val tier = buildTier (pageBytes)

        def read (key: Bytes): Option [Bytes] =
          tier.read (descriptor, key) .pass.flatMap (_.value)

        expectResult (Some (One)) (read (Apple))
        expectResult (Some (One)) (read (Orange))
        expectResult (Some (One)) (read (Watermelon))
        expectResult (None) (read (One))
      }

      "the pages are limited to one byte" in {
        checkFind (1)
      }

      "the pages are limited to 256 bytes" in {
        checkFind (1 << 6)
      }

      "the pages are limited to 64K" in {
        checkFind (1 << 16)
      }}}}
