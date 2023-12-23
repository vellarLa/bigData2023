package philosophers

import java.util.concurrent.Semaphore
import org.apache.zookeeper._
import scala.util.Random

case class Philosopher(id: Int,
                       hostPort: String,
                       root: String,
                       leftFork: Semaphore,
                       rightFork: Semaphore,
                       seats: Integer) extends Watcher {
  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val path: String = root + "/" + id.toString

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      //println(s"Event from keeper: ${event.getType}")
      mutex.notify()
    }
  }

  def eat(): Boolean = {
    printf("Philosopher %d wants to eat\n", id)
    mutex.synchronized {
      // создание эфимерного узла
      zk.create(path, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      // количество занятых мест за столом
      var occupiedSeats = zk.getChildren(root, this)
      // если свободных мест нет
      while (occupiedSeats.size() > seats) {
        //printf("Philosopher %d is waiting\n", id)
        zk.delete(path, -1)
        mutex.wait(3000)
        Thread.sleep(3000)
        zk.create(path, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        occupiedSeats = zk.getChildren(root, this)
      }
      printf("Philosopher %d is sitting\n", id)
      leftFork.acquire()
      printf("Philosopher %d picked up the left fork\n", id)
      rightFork.acquire()
      printf("Philosopher %d picked up the right fork\n", id)
      printf("Philosopher %d started eating\n", id)
      Thread.sleep((Random.nextInt(3) + 2) * 1000)
      rightFork.release()
      leftFork.release()
      printf("Philosopher %d finished eating\n", id)
      return true
    }
  false
  }

  def think(): Unit = {
    printf("Philosopher %d is thinking\n", id)
    zk.delete(path, -1)
    Thread.sleep((Random.nextInt(5) + 1) * 1000)
  }
}

