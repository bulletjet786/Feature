package zookeeper

import org.apache.zookeeper.*

fun main(args: Array<String>) {
    val client = Client("www.bulletjet.cn:2181/application/native")
    client.startZK()

    val name = client.queueCommand("echo")
    logger.debug("Created task $name")
}

class Client(private val connectionString: String): Watcher {
    private lateinit var zkc: ZooKeeper


    override fun process(event: WatchedEvent?) {
        logger.debug(event.toString())
    }

    fun startZK() {
        zkc = ZooKeeper(connectionString, SESSION_TIMEOUT, this)
    }

    fun queueCommand(command: String): String {
        while (true) {
            var name: String = ""
            try {
                name = zkc.create("/tasks/task-",
                        command.toByteArray(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL)
                return name
            } catch (e: KeeperException.NodeExistsException) {
                throw Exception("$name already appears to be running!")
            } catch (e: KeeperException.ConnectionLossException) {
            }
        }
    }

}