package it.polimi.genomics.executor

import grizzled.slf4j.Logging

/**
  * Created by canakoglu on 2/15/17.
  */
object CommandLineExecutor extends Executor with Logging {
  var executionScript: String = ""

  def apply(executionScriptIn:String) = {executionScript = executionScriptIn; CommandLineExecutor}

  override def execute(query: String): Boolean = {
    import sys.process._
    //        val exitCode = Seq("/home/canakoglu/datasetRepository/GMQL/bin/GMQL-Submit", "-script", query) !
    //
    //    println("exitCode: " + exitCode)

    val process = Process(executionScript, Seq("-script", query))
    val processLines = process.lines
    var hasError = false
    processLines.foreach { t =>
      if (t.contains("ERROR"))
        hasError = true
      logger.info(t)
    }

    if (hasError){
      logger.error("In the output of execution, there is error, please check it")

    }
    hasError
  }
}


object TestCommandLineExecuter extends App {
  CommandLineExecutor.execute("R = SELECT() /home/canakoglu/datasetRepository//datasets/beds_in/; \nMATERIALIZE R into /tmp/gmql_test_arif//select_test2/beds_out3/;")

}