package two_phase_commit

import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.Watcher.WatcherType
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{AddWatchMode, CreateMode, Transaction, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

case class Coordinator(hostPort: String, numClients: Int) extends Watcher{
  val zk = new ZooKeeper(hostPort, 300000, this)
  val mutex = new Object()
  val root = "/app/tx"
  if (zk == null) throw new Exception("ZK is NULL.")

  //zk.delete(root, -1)
  //zk.create(root, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  //zk.removeAllWatches(root, WatcherType.Children, false)
  zk.addWatch(root, AddWatchMode.PERSISTENT)
  val transaction: Transaction = zk.transaction()
  initTransaction()


  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, null)
        if (party.size() < numClients) {
          //mutex.wait()
        } else {
          var state = "COMMIT"
          for (id <- 0 until numClients) {
            val item = new String(zk.getData(root + "/" + id, false, null), "UTF-8")
            if (item.equals("ABORT")) {
              state = "ABORT"
            }
          }
          for (id <- 0 until numClients) {
            zk.setData(root + "/" + id, state.getBytes, -1)
          }
          mutex.notifyAll()
          return

        }
        mutex.notifyAll()
        return false
      }
    }

  }

  def initTransaction(): Unit = {
    val threads = new Array[Thread](numClients)
    for (id <- 0 until  numClients) {
      threads(id) = new Thread(
        () => {
          val client = Client(id, hostPort, "/app/tx", transaction)
          client.clientCreate()
          Thread.sleep(9000)
          //client.delete()

        }
      )
      threads(id ).setDaemon(false)
      threads(id).start()
    }
    for (id <- 0 until numClients) {
      threads(id).join()
    }

  }

}
