package example

import com.twitter.finagle.transport._
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.{Stack, Status}
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.{Closable, Future, Promise, Time, Throw, Return, Duration}
import java.net.SocketAddress


/**
 * A collection of [[com.twitter.finagle.Stack.Param]]'s useful for configuring
 * a [[com.twitter.finagle.transport.Transport]].
 *
 * @define $param a [[com.twitter.finagle.Stack.Param]] used to configure
 */
object Transport {
  /**
   * $param the buffer sizes of a `Transport`.
   *
   * @param send An option indicating the size of the send buffer.
   * If None, the implementation default is used.
   *
   * @param recv An option indicating the size of the receive buffer.
   * If None, the implementation default is used.
   */
  case class BufferSizes(send: Option[Int], recv: Option[Int])
  implicit object BufferSizes extends Stack.Param[BufferSizes] {
    val default = BufferSizes(None, None)
  }

  /**
   * $param the liveness of a `Transport`. These properties dictate the
   * lifecycle of a `Transport` and ensure that it remains relevant.
   *
   * @param readTimeout A maximum duration a listener is allowed
   * to read a request.
   *
   * @param writeTimeout A maximum duration a listener is allowed to
   * write a response.
   *
   * @param keepAlive An option indicating if the keepAlive is on or off.
   * If None, the implementation default is used.
   */
  case class Liveness(
    readTimeout: Duration,
    writeTimeout: Duration,
    keepAlive: Option[Boolean]
  )
  implicit object Liveness extends Stack.Param[Liveness] {
    val default = Liveness(Duration.Top, Duration.Top, None)
  }

  /**
   * $param the verbosity of a `Transport`. Transport activity is
   * written to [[com.twitter.finagle.param.Logger]].
   */
  case class Verbose(b: Boolean)
  implicit object Verbose extends Stack.Param[Verbose] {
    val default = Verbose(false)
  }

  /**
   * $param the TLS engine for a `Transport`.
   */
  case class TLSClientEngine(e: Option[SocketAddress => com.twitter.finagle.ssl.Engine])
  implicit object TLSClientEngine extends Stack.Param[TLSClientEngine] {
    val default = TLSClientEngine(None)
  }

  /**
   * $param the TLS engine for a `Transport`.
   */
  case class TLSServerEngine(e: Option[() => com.twitter.finagle.ssl.Engine])
  implicit object TLSServerEngine extends Stack.Param[TLSServerEngine] {
    val default = TLSServerEngine(None)
  }

  /**
   * Serializes the object stream from a `Transport` into a
   * [[com.twitter.io.Writer]].
   *
   * The serialization function `f` can return `Future.None` to interrupt the
   * stream to faciliate using the transport with multiple writers and vice
   * versa.
   *
   * Both transport and writer are unmanaged, the caller must close when
   * done using them.
   *
   * {{{
   * copyToWriter(trans, w)(f) ensure {
   *   trans.close()
   *   w.close()
   * }
   * }}}
   *
   * @param trans The source Transport.
   *
   * @param writer The destination [[com.twitter.io.Writer]].
   *
   * @param f A mapping from `A` to `Future[Option[Buf]]`.
   */
  def copyToWriter[A](trans: Transport[_, A], w: Writer)
                     (f: A => Future[Option[Buf]]): Future[Unit] = {
    trans.read().flatMap(f).flatMap {
      case None => Future.Done
      case Some(buf) => w.write(buf) before copyToWriter(trans, w)(f)
    }
  }

  /**
   * Collates a transport, using the collation function `chunkOfA`,
   * into a [[com.twitter.io.Reader]].
   *
   * Collation completes when `chunkOfA` returns `Future.None`. The returned
   * [[com.twitter.io.Reader]] is also a Unit-typed
   * [[com.twitter.util.Future]], which is satisfied when collation
   * is complete, or else has failed.
   *
   * @note This deserves its own implementation, independently of
   * using copyToWriter. In particular, in today's implemenation,
   * the path of interrupts are a little convoluted; they would be
   * clarified by an independent implementation.
   */
  def collate[A](trans: Transport[_, A], chunkOfA: A => Future[Option[Buf]])
  : Reader with Future[Unit] = new Promise[Unit] with Reader {
    private[this] val rw = Reader.writable()
    become(Transport.copyToWriter(trans, rw)(chunkOfA) respond {
      case Throw(exc) => rw.fail(exc)
      case Return(_) => rw.close()
    })

    def read(n: Int) = rw.read(n)

    def discard() {
      rw.discard()
      raise(new Reader.ReaderDiscarded)
    }
  }
}

/**
 * A factory for transports: they are specially encoded as to be
 * polymorphic.
 */
trait TransportFactory {
  def apply[In, Out](): Transport[In, Out]
}

/**
 * A `Transport` interface to a pair of queues (one for reading, one
 * for writing); useful for testing.
 */
class QueueTransport[In, Out](writeq: AsyncQueue[In], readq: AsyncQueue[Out])
  extends Transport[In, Out]
{
  private[this] val closep = new Promise[Throwable]

  def write(input: In) = {
    writeq.offer(input)
    Future.Done
  }
  def read(): Future[Out] =
    readq.poll() onFailure { exc =>
      closep.setValue(exc)
    }
  def status = if (closep.isDefined) Status.Closed else Status.Open
  def close(deadline: Time) = {
    val ex = new IllegalStateException("close() is undefined on QueueTransport")
    closep.updateIfEmpty(Throw(ex))
    Future.exception(ex)
  }

  val onClose = closep
  val localAddress = new SocketAddress{}
  val remoteAddress = new SocketAddress{}
}

