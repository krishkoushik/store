package com.treode.store.local.disk.timed

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, Fruits}
import com.treode.store.local.{TimedCell, TimedIterator, TimedTestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana}

class DeletesFilterSpec extends FlatSpec with TimedTestTools {

  private def expectCells (cs: TimedCell*) (actual: TimedIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: TimedCell*) = {
    var iter: TimedIterator = null
    DeletesFilter (TimedIterator.adapt (cs.iterator), new Callback [TimedIterator] {
      def pass (_iter: TimedIterator) = iter = _iter
      def fail (t: Throwable) = throw t
    })
    assert (iter != null)
    iter
  }

  "The DeletesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple##2]" in {
    expectCells () (newFilter (Apple##2))
  }

  it should "handle [Apple##2::1]" in {
    expectCells (Apple##2::1) (newFilter (Apple##2::1))
  }

  it should "handle [Apple##2::1, Apple##1]" in {
    expectCells (Apple##2::1) (newFilter (Apple##2::1, Apple##1))
  }

  it should "handle [Apple##2, Apple##1::1]" in {
    expectCells (Apple##2, Apple##1::1) (newFilter (Apple##2, Apple##1::1))
  }

  it should "handle [Apple##2::1, Apple##1::1]" in {
    expectCells (Apple##2::1, Apple##1::1) (newFilter (Apple##2::1, Apple##1::1))
  }

  it should "handle [Apple##2, Banana##2]" in {
    expectCells () (newFilter (Apple##2, Banana##2))
  }

  it should "handle [Apple##2::1, Banana##2]" in {
    expectCells (Apple##2::1) (newFilter (Apple##2::1, Banana##2))
  }

  it should "handle [Apple##2::1, Apple##1, Banana##2]" in {
    expectCells (Apple##2::1) (newFilter (Apple##2::1, Apple##1, Banana##2))
  }

  it should "handle [Apple##2, Apple##1::1, Banana##2]" in {
    expectCells (Apple##2, Apple##1::1) (newFilter (Apple##2, Apple##1::1, Banana##2))
  }

  it should "handle [Apple##2::1, Apple##1::1, Banana##2]" in {
    expectCells (Apple##2::1, Apple##1::1) (
        newFilter (Apple##2::1, Apple##1::1, Banana##2))
  }

  it should "handle [Apple##2, Banana##2::1]" in {
    expectCells (Banana##2::1) (newFilter (Apple##2, Banana##2::1))
  }

  it should "handle [Apple##2::1, Banana##2::1]" in {
    expectCells (Apple##2::1, Banana##2::1) (newFilter (Apple##2::1, Banana##2::1))
  }

  it should "handle [Apple##2::1, Apple##1, Banana##2::1]" in {
    expectCells (Apple##2::1, Banana##2::1) (
        newFilter (Apple##2::1, Apple##1, Banana##2::1))
  }

  it should "handle [Apple##2, Apple##1::1, Banana##2::1]" in {
    expectCells (Apple##2, Apple##1::1, Banana##2::1) (
        newFilter (Apple##2, Apple##1::1, Banana##2::1))
  }

  it should "handle [Apple##2::1, Apple##1::1, Banana##2::1]" in {
    expectCells (Apple##2::1, Apple##1::1, Banana##2::1) (
        newFilter (Apple##2::1, Apple##1::1, Banana##2::1))
  }

  it should "handle [Apple##2, Banana##2::1, Banana##1]" in {
    expectCells (Banana##2::1) (newFilter (Apple##2, Banana##2::1, Banana##1))
  }

  it should "handle [Apple##2::1, Banana##2::1, Banana##1]" in {
    expectCells (Apple##2::1, Banana##2::1) (
        newFilter (Apple##2::1, Banana##2::1, Banana##1))
  }

  it should "handle [Apple##2::1, Apple##1, Banana##2::1, Banana##1]" in {
    expectCells (Apple##2::1, Banana##2::1) (
        newFilter (Apple##2::1, Apple##1, Banana##2::1, Banana##1))
  }

  it should "handle [Apple##2, Apple##1::1, Banana##2::1, Banana##1]" in {
    expectCells (Apple##2, Apple##1::1, Banana##2::1) (
        newFilter (Apple##2, Apple##1::1, Banana##2::1, Banana##1))
  }

  it should "handle [Apple##2::1, Apple##1::1, Banana##2::1, Banana##1]" in {
    expectCells (Apple##2::1, Apple##1::1, Banana##2::1) (
        newFilter (Apple##2::1, Apple##1::1, Banana##2::1, Banana##1))
  }}