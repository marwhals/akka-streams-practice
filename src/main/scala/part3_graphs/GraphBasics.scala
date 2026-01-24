package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}
import akka.stream.{ActorMaterializer, ClosedShape}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Notes:
 *
 * Non-linear components
 * - fan-out
 * - fan-in
 *
 * Fan-out components
 * - Broadcast
 * - Balance
 *
 * Fan-in components
 * - Zip/ZipWith
 * - Merge
 * - Concat
 */

object GraphBasics extends App {

  implicit val system = ActorSystem("GraphBasics")
  implicit val materializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(x => x + 1) // "hard" computation
  val multiplier = Flow[Int].map(x => x * 10) // "hard" computation
  val output = Sink.foreach[(Int, Int)](println)

  // step 1 - setting up the fundamentals for the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // builder is a *mutable* data structure
      import GraphDSL.Implicits._ // brings some nice operators into scope

      // step 2 - add the necessary components of this graph
      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator
      val zip = builder.add(Zip[Int, Int]) // fan-in operator

      // step 3 - tying up the components
      input ~> broadcast // ~> ---- can be read as input feeds into broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1

      zip.out ~> output

      // step 4 - return a closed shape
      ClosedShape // Here the builders shape is "frozen"
      // shape object
    } // inert static graph
  ) // runnable graph

  //  graph.run() // run the graph and materialize it

  /**
   * Exercise 1: feed a source into 2 sinks at the same time (hint: use a broadcast)
   */

  val firstSink = Sink.foreach[Int](x => println(s"First sink: $x"))
  val secondSink = Sink.foreach[Int](x => println(s"Second sink: $x"))

  // step 1
  val sourceToTwoSinksGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 - declaring the components
      val broadcast = builder.add(Broadcast[Int](2))

      // step 3 - tying up the components
      input ~> broadcast ~> firstSink // automatically allocate ports for the broadcast into each sink -- known as implicit port numbering
      broadcast ~> secondSink
      //      broadcast.out(0) ~> firstSink
      //      broadcast.out(1) ~> secondSink

      //step 4
      ClosedShape

    }
  )

  /**
   * Exercise 2: Balance
   */

  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)

  val sink1 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1 number of elements: $count")
    count + 1
  })

  val sink2 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 2 number of elements: $count")
    count + 1
  })

  // step 1
  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      // step 2 --- declare components
      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](2)) // Events out the rate of production between sources

      // step 3 --- tie them up
      fastSource ~> merge ~> balance ~> sink1
      slowSource ~> merge
      balance ~> sink2

      // step 4
      ClosedShape
    }
  )

  balanceGraph.run()

}
