package part2_primer

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
 * Goal
 * - Stream components running on the same actor
 * - Async boundaries between stream components
 *
 * Async boundaries - see diagram
 * - An async boundary contains
 * --- Everything from the previous boundary
 * --- Everything between the previous boundary and this boundary
 *
 * - Communication is based on asynchronous actor messages
 *
 * Recap
 * - Akka Streams components are fused i.e - they run on the same actor
 *
 * Async Boundaries
 * - components run on different actors
 * - better throughput
 *
 * Best when: Individual operations are expensive
 * Avoid when: operations are comparable with a message pass
 *
 * Order guarantees
 *
 */

object OperatorFusion extends App {

  implicit val system = ActorSystem("OperatorFusion")
  implicit val materializer = ActorMaterializer()

  val simpleSource = Source(1 to 1000)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleFlow2 = Flow[Int].map(_ * 10)
  val simpleSink = Sink.foreach[Int](println)

  // this runs on the >>>same actor<<<
  //  simpleSource.via(simpleFlow).via(simpleFlow2).to(simpleSink) //<--- operator/ component fusion - akka streams does this behind the scenes to improve throughput
  //    .run() // this will be done on a single core

  // "equivalent" behavior -- runs on a single core -- consider async message passing
  class SimpleActor extends Actor {
    override def receive: Receive = {
      case x: Int =>
        // flow operations
        val x2 = x + 1
        val y = x2 * 10
        // sink operation
        println(y)
    }
  }

  val simpleActor = system.actorOf(Props[SimpleActor])
  //  1.to(1000).foreach(simpleActor ! _)

  // complex flow:
  val complexFlow = Flow[Int].map { x =>
    // simulating a long computation
    Thread.sleep(1000)
    x + 1
  }

  val complexFlow2 = Flow[Int].map { x =>
    Thread.sleep(1000)
    x * 10
  }

  //  simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink) // NOTE: These run on the same boundary
  //    .run()

  // async boundary -- run on different actors -- breaks the operator fusion which akka streams does by default
  simpleSource.via(complexFlow).async // run on one actor
    .via(complexFlow2).async // runs on another actor
    .to(simpleSink) // runs on a third actor
    .run()

  // order guarantees - operators are fused until .async is added. .async will lead to relative ordering i.e ordering within each flow. Different flows interleave
  Source(1 to 3)
    .map(element => { println(s"Flow A: $element"); element}).async
    .map(element => { println(s"Flow B: $element"); element}).async
    .map(element => { println(s"Flow C: $element"); element}).async
    .runWith(Sink.ignore)

}