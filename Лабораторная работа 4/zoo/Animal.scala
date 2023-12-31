package zoo

import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

case class Animal(name: String, hostPort: String, root: String, partySize: Integer) extends Watcher {

  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val animalPath: String = root + "/" + name

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {

    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
      mutex.notify()
    }
  }

  // реализация метода enter
  def enter(): Boolean = {

    // создание эфимерного узла
    zk.create(animalPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    // блок синхронизации,ожидающий появления в корневом узле /zoo всех животных
    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for the others.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
      return false
    }
  }

  // удаление эфимерного узла
  def delete(): Unit = {
    zk.delete(animalPath, -1)
  }

}