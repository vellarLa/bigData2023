package two_phase_commit

import java.util.concurrent.Semaphore
import ch.qos.logback.classic.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level

object Main {
  def main(args: Array[String]): Unit = {
    val Seq(clientsNum, hostPort) = args.toSeq
    val coordinator = Coordinator(hostPort, clientsNum.toInt)

  }
}

