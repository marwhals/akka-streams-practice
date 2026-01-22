package part2_primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

/**
 * Materializing a graph = materializing all components
 * - each component produces a materialized value when run
 * - the graph produces a >>>single<<< materialized value
 * - programmers job is to choose which one to pick
 *
 * A component can materialize multiple times
 * - A materialized value can be anything
 *
 */

import scala.util.{Failure, Success}

/**
 * Goal - getting a meaningful value out of a running stream
 *
 * Materializing
 * - Components are static until they are run
 * - A graph is a "blueprint" for a stream
 * - Running a group allocates the right resources
 *  - actors, thread pools
 *  - socket, connections
 *  - etc - everything is transparent
 *
 * Running a graph is also called materializing --> the result is called a materialized value
 *
 * Materialized Values
 * - Materializing a graph = materializing all components
 * --- each component produces a materialized value when it has been run TODO add diagram
 * --- the graph produces a single materialized value
 * --- our job to choose which one to pick
 *
 * - A component can materialize multiple times
 * --- you can reuse the same component in different graphs
 * --- different runs = different materialization's!
 *
 * - A materialized value can be *anything*
 *
 *
 */

object MaterializingStreams extends App {

  implicit val system = ActorSystem("MaterializingStreams")
  implicit val materialzer = ActorMaterializer()
  import system.dispatcher

  val simpleGraph = Source(1 to 10).to(Sink.foreach(println))
//  val simpleMaterializedValue = simpleGraph.run()

  val source = Source(1 to 10)
  val sink = Sink.reduce[Int]((a, b) => a + b)
//  val sumFuture = source.runWith(sink)
//  sumFuture.onComplete {
//    case Success(value) => println(s"The sum of all elements is :$value")
//    case Failure(ex) => println(s"The sum of the element could not be computed: $ex")
//  }

  // choosing materialized values
  val simpleSource = Source(1 to 10)
  val simpleFlow = Flow[Int].map(x => x + 1)
  val simpleSink = Sink.foreach[Int](println)
  val graph = simpleSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right)
  graph.run().onComplete {
    case Success(_) => println("Stream processing finished")
    case Failure(exception) => println(s"Stream processing failed with: $exception")
  }

  // sugars
  Source(1 to 10).runWith(Sink.reduce(_ + _)) // source.to(Sink.reduce)(Keep.right)
  Source(1 to 10).runReduce(_ + _) // same

  // backwards
  Sink.foreach[Int](println).runWith(Source.single(42)) // source(..).to(sink...).run()

  //both ways
  Flow[Int].map(x => 2 * x).runWith(simpleSource, simpleSink)

  /**
   * - return the last element out of a source (user Sink.last)
   * - compute the total word court out of a stream of sentences
   *  - map, fold, reduce
   */
  val f1 = Source(1 to 10).toMat(Sink.last)(Keep.right).run()
  val f2 = Source(1 to 10).runWith(Sink.last) // same as the line above

  val sentenceSource = Source(List(
    "Akka is awesome",
    "I love streams",
    "Materialized values are killing me"
  ))
  val wordCountSink = Sink.fold[Int, String](0)((currentWords, newSentence) => currentWords + newSentence.split(" ").length)
  val g1 = sentenceSource.toMat(wordCountSink)(Keep.right).run()
  val g2 = sentenceSource.runWith(wordCountSink)
  val g3 = sentenceSource.runFold(0)((currentWords, newSentence) => currentWords + newSentence.split(" ").length)

  val wordCountFlow = Flow[String].fold[Int](0)((currentWords, newSentence) => currentWords + newSentence.split(" ").length)
  val g4 = sentenceSource.via(wordCountFlow).toMat(Sink.head)(Keep.right).run()
  val g5 = sentenceSource.viaMat(wordCountFlow)(Keep.left).toMat(Sink.head)(Keep.right).run()
  val g6 = sentenceSource.via(wordCountFlow).runWith(Sink.head)
  val g7 = wordCountFlow.runWith(sentenceSource, Sink.head)._2


}
