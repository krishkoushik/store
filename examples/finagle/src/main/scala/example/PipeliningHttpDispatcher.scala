package example

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.http._
import ReaderUtils.{readChunk, streamChunks}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{StatsReceiver, DefaultStatsReceiver, RollupStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.util._
import com.twitter.finagle.util.Throwables
import com.twitter.io.{Reader, Buf, BufReader}
import com.twitter.logging.Logger
import java.net.InetSocketAddress
import com.twitter.finagle.http.codec._
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.dispatch._
import com.twitter.finagle.{Service, NoStacktrace, CancelledRequestException}
import java.util.concurrent.atomic.AtomicReference

class NewHttpServerDispatcher[REQUEST <: Request](
    trans: Transport[Any, Any],
    service: Service[REQUEST, HttpResponse],
    stats: StatsReceiver)
  extends HttpServerDispatcher(trans, service, stats) {

    import NewGenSerialServerDispatcher._

    private[this] val state = new AtomicReference[Future[_]](Idle)
    private[this] val cancelled = new CancelledRequestException

    private[this] def loop(): Future[Unit] = {
      println ("Inside the defined loop :) HAHAHA")
        state.set(Idle)
          trans.read() flatMap { req =>
            val p = new Promise[HttpResponse]
              if (state.compareAndSet(Idle, p)) {
                val eos = new Promise[Unit]
                  val save = Local.save()
                  try p.become(dispatch(req, eos))
                  finally Local.restore(save)
                  p map { res => (res, eos) }
              } else Eof
          } flatMap { case (rep, eos) =>
            Future.join(handle(rep), eos).unit
          } respond {
            case Return(()) if state.get ne Closed =>
              loop()
              case _ =>
              trans.close()
          }
      }
  }
