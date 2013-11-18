package com.treode.store.local.disk.simple

import com.treode.pickle.{Buffer, Picklers, pickle, unpickle}
import com.treode.store.{Bytes, Fruits}
import com.treode.store.local.{SimpleCell, SimpleTestTools}
import org.scalatest.WordSpec

import Fruits.{Apple, Banana, Kiwi, Kumquat, Orange}
import SimpleTestTools._

class SimpleCellPageSpec extends WordSpec {

  private def newPage (entries: SimpleCell*): CellPage =
    new CellPage (Array (entries: _*))

  private def entriesEqual (expected: SimpleCell, actual: SimpleCell) {
    expectResult (expected.key) (actual.key)
    expectResult (expected.value) (actual.value)
  }

  private def pagesEqual (expected: CellPage, actual: CellPage) {
    expectResult (expected.entries.length) (actual.entries.length)
    for (i <- 0 until expected.entries.length)
      entriesEqual (expected.entries (i), actual.entries (i))
  }

  private def checkPickle (page: CellPage) {
    val buffer = Buffer (12)
    pickle (CellPage.pickle, page, buffer)
    val result = unpickle (CellPage.pickle, buffer)
    pagesEqual (page, result)
  }

  "A simple CellPage" when {

    "empty" should {

      val page = newPage ()

      "find nothing" in {
        expectResult (0) (page.find (Apple))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding one entry k=kiwi" should {

      val page = newPage (Kiwi::None)

      "find apple before kiwi" in {
        expectResult (0) (page.find (Apple))
      }

      "find kiwi using kiwi" in {
        expectResult (0) (page.find (Kiwi))
      }

      "find orange after kiwi" in {
        expectResult (1) (page.find (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}

    "holding three entries" should {

      val page = newPage (Apple::None, Kiwi::None, Orange::None)

      "find apple using apple" in {
        expectResult (0) (page.find (Apple))
      }

      "find kiwi using banana" in {
        expectResult (1) (page.find (Banana))
      }

      "find kiwi using kiwi" in {
        expectResult (1) (page.find (Kiwi))
      }

      "find orange using kumquat" in {
        expectResult (2) (page.find (Kumquat))
      }

      "find orange using orange" in {
        expectResult (2) (page.find (Orange))
      }

      "pickle and unpickle to the same value" in {
        checkPickle (page)
      }}}}
