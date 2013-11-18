package com.treode.store

private class PrepareCaptor extends PrepareCallback {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: Transaction = null
  private var _t: Throwable = null
  private var _ks: Set [Int] = null
  private var _advance = false

  private def notInvoked() {
    if (_invokation == null) {
      _invokation = Thread.currentThread.getStackTrace
    } else {
      val _second = Thread.currentThread.getStackTrace
      println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
      println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
      assert (false, "PrepareCallback was already invoked.")
    }}

  def pass (v: Transaction) {
    notInvoked()
    _v = v
  }

  def fail (t: Throwable) {
    notInvoked()
    _t = t
  }

  def collisions (ks: Set [Int]) {
    notInvoked()
    _ks = ks
  }

  def advance() {
    notInvoked()
    _advance = true
  }

  private def invoked() {
    assert (_invokation != null, "PrepareCallback was not invoked.")
  }

  def passed: Transaction = {
    invoked()
    assert (_v != null, "PrepareCallback did not pass.")
    _v
  }

  def failed: Throwable = {
    invoked()
    assert (_t != null, "PrepareCallback did not fail.")
    _t
  }

  def collided: Set [Int] = {
    invoked()
    assert (_ks != null, "PrepareCallback did not collide.")
    _ks
  }

  def advanced: Boolean = {
    invoked()
    assert (_advance, "PrepareCallback did not advance.")
    _advance
  }}