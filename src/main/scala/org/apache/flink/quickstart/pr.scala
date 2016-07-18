package org.apache.flink.quickstart

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.library.PageRank
import org.apache.flink.graph.scala.utils.Tuple3ToEdgeMap
import org.apache.flink.graph._
import java.lang.{Double => JDouble}

/**
  *
  * The edges input file is expected to contain one edge per line, with String IDs and double
  * values in the following format:"<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
  *
  * This class is used to create a graph from the input data and then to run a PageRankAlgorithm
  * (present in Flink-gelly graph-library)over it. The algorithm used is a simplified implementation
  * of the actual algorithm; its limitation is that all the pages need to have at least one incoming
  * and one outgoing link for correct results. The vertex-centric algorithm takes as input parameters
  * dampening factor and number of iterations.
  *
  */

object pr {

  private val DAMPENING_FACTOR: Double = 0.85

  private var edgesInputPath: String = "/media/clm/KING16-3/maestria/TESIS/CODE/08prgov/data/tab_sFile.txt"
  //private var edgesInputPath: String = "/media/clm/KING16-2/exer/flink/flink-scala-project/data/res.txt"
  //private var edgesInputPath: String = "data/tab_sFile.txt"
  private var outputPath: String = "/media/clm/KING16-3/maestria/TESIS/CODE/08prgov/data/resPR.txt"
  //private var outputPath: String = "/media/clm/KING16-2/exer/flink/flink-scala-project/data/resPR.txt"
  //private var outputPath: String = "data/resPR.txt"
  private var maxIterations: Int = 10

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //read the Edge DataSet from the input file (Tuple2)
    val links = env.readCsvFile[(String, String, JDouble)](
      edgesInputPath,
      fieldDelimiter = "\t",
      lineDelimiter = "\n")
      .map(new Tuple3ToEdgeMap[String, JDouble]()
      )
    //println ("mails:")
    links.first(3).print()

    //create a Graph with vertex values initialized to 1.0
    val network = org.apache.flink.graph.scala.Graph
      .fromDataSet(links, new MapFunction[String, JDouble]() {
        def map(value: String): JDouble = { 1.0 }
      }, env)

    val sumEdgeWeights = network.reduceOnEdges(new SumWeight(), EdgeDirection.OUT)

    // assign the transition probabilities as edge weights:
    // divide edge weight by the total weight of outgoing edges for that source
    val networkWithWeights = network.joinWithEdgesOnSource(sumEdgeWeights,
      new EdgeJoinFunction[JDouble, JDouble]() {
        def edgeJoin(d1: JDouble, d2: JDouble) = {
          d1 / d2
        }
      })

    //Now run the Page Rank algorithm over the weighted graph
    val pageRanks = networkWithWeights
      .run(new PageRank[String](DAMPENING_FACTOR, maxIterations))

    pageRanks.writeAsCsv(outputPath, "\n", "\t")

    env.execute("Run PageRank with Edge Weights")

  } // end main

  private def parseParameters(args: Array[String]): Boolean = {
    if(args.length > 0) {
      if(args.length != 3) {
        System.err.println("Usage PageRankWithEdgeWeights <edge path> <output path> <num iterations>")
        false
      }
      edgesInputPath = args(0)
      outputPath = args(1)
      maxIterations = 2
    }
    true
  }
}

//function to calculate the total weight of outgoing edges from a node
class SumWeight extends ReduceEdgesFunction[JDouble] {
  override def reduceEdges(firstEdgeValue: JDouble, secondEdgeValue: JDouble): JDouble = firstEdgeValue
}