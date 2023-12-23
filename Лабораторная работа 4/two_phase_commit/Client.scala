package two_phase_commit

import org.apache.zookeeper.{AddWatchMode, CreateMode, Transaction, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import scala.util.Random

case class Client(id: Int, hostPort: String, root: String, transaction: Transaction) extends Watcher {
  val zk = new ZooKeeper(hostPort, 300000, this)
  val mutex = new Object()
  private val clientPath: String = root + "/" + id
  private var STATUS = Array.empty[String]
  STATUS = STATUS.appended("COMMIT")
  STATUS = STATUS.appended("ABORT")
  STATUS = STATUS.appended("COMMITTED")
  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      if (event.getType.eq(Watcher.Event.EventType.NodeDataChanged)) {
        println("NEW_STATUS - " + new String(zk.getData(clientPath, false, null), "UTF-8"))
        if (new String(zk.getData(clientPath, false, null), "UTF-8").equals(STATUS(0))) {
          transaction.commit()
        }
        else {
          transaction.delete(clientPath, -1)
        }
        zk.setData(clientPath, STATUS(2).getBytes, -1)
        zk.setData(root, STATUS(2).getBytes, -1)
        delete()
        mutex.notify()
      }
    }
  }

  def clientCreate(): Boolean = {
    val idx = Random.nextInt(2)
    println(s"Client - " + STATUS(idx))
    zk.create(clientPath, STATUS(idx).getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    //Thread.sleep(2000)
    zk.addWatch(clientPath, AddWatchMode.PERSISTENT)
    false
  }

  // удаление эфимерного узла
  def delete(): Unit = {
    zk.delete(clientPath, -1)
  }

}
