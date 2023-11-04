package uk.co.odinconsultants

import uk.co.odinconsultants.SparkUtils.{getSession, write}

import java.util.concurrent.TimeUnit.MINUTES
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** Should fail with something like:
  * Exception in thread "main" io.delta.exceptions.ConcurrentAppendException: Files were added to the root of the table by a concurrent update. Please try the operation again.
  * Conflicting commit: {"timestamp":1699088385118,"operation":"WRITE","operationParameters":{"mode":Overwrite,"partitionBy":[]},"readVersion":2,"isolationLevel":"Serializable","isBlindAppend":false,"operationMetrics":{"numFiles":"2","numOutputRows":"5","numOutputBytes":"977"},"engineInfo":"Apache-Spark/3.5.0 Delta-Lake/3.0.0","txnId":"775eb0f3-2f17-48d1-8576-d58de680cb12"}
  */
object ConflictingWriteMain {

  def main(args: Array[String]): Unit = {
    val first   = Future(writeData("phtest"))
    val second  = Future(writeData("phtest2"))
    val success = for {
      future <- List(first, second)
    } yield Await.result(future, Duration(1, MINUTES))
    println(success.mkString(", "))
  }

  private def writeData(app: String) =
    write(getSession(app))

}
