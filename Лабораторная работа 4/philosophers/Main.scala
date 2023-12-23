package philosophers


import java.util.concurrent.Semaphore
import scala.util.Random
import ch.qos.logback.classic.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level

object Main {
  val root = LoggerFactory.getLogger("org.apache.http.wire").asInstanceOf[Logger]
  root.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    println("Starting dinner...")

    val Seq(philosophersCount, hostPort, numEats) = args.toSeq
    val forksCount = philosophersCount.toInt
    val seats = philosophersCount.toInt - 1

    // вилки - это семафоры
    val forks = new Array[Semaphore](forksCount)
    for (i <- 0 until forksCount) {
      forks(i) = new Semaphore(1)
    }

    // философы - это потоки
    val threads = new Array[Thread](philosophersCount.toInt)
    for (id <- 1 to philosophersCount.toInt) {
      threads(id - 1) = new Thread(
        // переопределяем метод run
        () => {
          // создание философа
          val philosopher = Philosopher(id, hostPort, "/philosopher", forks(id - 1),
            forks(id % philosophersCount.toInt), seats)

          // приемы пищи
          for (i <-1 to numEats.toInt) {
            philosopher.eat()
            philosopher.think()
          }
        }
      )
      // поток важен - есть необходимость ждать пока он выполнится
      threads(id - 1).setDaemon(false)
      threads(id - 1).start()
    }

    // будем ждать пока потоки закончат свое действие
    for (id <- 0 until philosophersCount.toInt) {
      threads(id).join()
    }

  }
}
