import calico.*
import calico.html.io.{*, given}
import cats.effect.*
import fs2.concurrent.SignallingRef
import fs2.dom.*
import fs2.*

import scala.concurrent.duration.given
import cats.syntax.all.*

import scala.annotation.tailrec
import scala.collection.immutable.Queue

extension [A](q: Queue[A])
  @tailrec
  private def dequeueN(n: Int): Queue[A] =
    if n > 0 then q.dequeue._2.dequeueN(n - 1) else q

def echo = for
  maxSize         <- SignallingRef.of[IO, Int](128).toResource
  ref             <- SignallingRef.of[IO, Option[String]](None).toResource
  continousValues <- Stream
    .fixedRate[IO](100.millis)
    .evalMap(_ => (ref.get.map(Chunk.fromOption), maxSize.get).mapN(_.tupleLeft(_)))
    .unchunks
    .map((m, s) => (m, Queue(span(s), br(())).map(_.widen[fs2.dom.Node[IO]])))
    .scan(Queue.empty[Resource[IO, fs2.dom.Node[IO]]]): (q1, q2) =>
      val max = q2._1
      val q   = q1.enqueueAll(q2._2)
      if q.size > max then q.dequeueN(q.size - max) else q
    .map(_.reverse)
    .holdOptionResource
    .map(_.map(_.toList.flatten))
  res <- div(
    input.withSelf: self =>
      (
        placeholder := "Echo?",
        onInput --> (_.foreach(e => self.value.get.flatMap(r => ref.set(Option.when(r.nonEmpty)(r)))))
      ),
    input.withSelf: self =>
      (
        tpe   := "number",
        value := "128",
        onInput --> (_.foreach(e => self.value.get.map(_.toIntOption).flatMap(_.traverse_(maxSize.set))))
      ),
    children <-- continousValues
  )
yield res
