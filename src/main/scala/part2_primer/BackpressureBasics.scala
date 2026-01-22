package part2_primer

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Backpressure
 * - One of the fundamental features of Reactive Streams
 * - Elements flow as a response to demand from consumers
 * TODO - see diagram
 * - Backpressure is all about speed in between these async components
 * - Fast consumers: All is well
 * - Slow consumer: problem
 * --- consumer will send a signal to producer to slow down
 * - Backpressure protocol is transparent
 *
 * Backpressure is mostly about slowing down a fast producer in the presence of a slow consumer
 *
 * --------
 * Data flows through stream in response for demand
 * Akka streams can slow down fast producers
 * Backpressure protocol is transparent
 *
 *
 */

object BackpressureBasics extends App {

  implicit val system = ActorSystem("BackpressureBasics")
  implicit val materializer = ActorMaterializer()

  val fastSource = Source(1 to 1000)
  val slowSink = Sink.foreach[Int] { x =>
    // simulate a long processing
    Thread.sleep(1000)
    println(s"Sink: $x")
  }

  fastSource.to(slowSink).run() // this is operator fusion. This is not backpressure

  fastSource.async.to(slowSink).run() // backpressure

  val simpleFlow = Flow[Int].map { x =>
    println(s"Incoming: $x")
    x + 1
  }

  fastSource
    .async.via(simpleFlow) // every one of these components operates on a different actor
    .async.to(slowSink)
    .run()

  // an akka streams component can have multiple components to backpressure signals

  /**
   * Reactions to backpressure (in order)
   * - try to slow down if possible
   * - buffer elements until there's more demand
   * - drop down elements from the buffer if it overflows <--- only have control at this point
   * - tear down/kill the whole stream (failure)
   */

  val bufferedFlow = simpleFlow.buffer(10 , overflowStrategy = OverflowStrategy.dropHead)
  fastSource.async
    .via(bufferedFlow).async
    .to(slowSink)
    .run()

  /*
    overflow strategies
    - drop head = oldest
    - drop tail = newest
    - drop new = exact element to be added = keeps the buffer
    - drop the entire buffer
    - backpressure signal
    - fail
   */

  // throttling
  import scala.concurrent.duration._
  fastSource.throttle(2,1 second).runWith(Sink.foreach(println))


}
