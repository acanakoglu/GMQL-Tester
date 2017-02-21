package it.polimi.genomics.executor

/**
  * Created by canakoglu on 1/26/17.
  */
trait Executor {

  def execute(query: String): Boolean

}
