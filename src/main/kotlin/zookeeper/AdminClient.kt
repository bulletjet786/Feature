package zookeeper

import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import java.util.*

fun main(args: Array<String>) {

    val adminClient = AdminClient("www.bulletjet.cn:2181/application/native")
    adminClient.startZK()
    adminClient.listState()
    adminClient.stop()
}

class AdminClient(private val connectionString: String): Watcher {
    private lateinit var zkc: ZooKeeper

    fun stop() {
        zkc.close()
    }

    override fun process(event: WatchedEvent?) {
        logger.debug(event.toString())
    }

    fun startZK() {
        zkc = ZooKeeper(connectionString, SESSION_TIMEOUT, this)
    }

    fun listState() {
        try {
            var stat = Stat()
            val masterData = zkc.getData("/master", false, stat)
            val startData = Date(stat.ctime)
            println("Master: ${String(masterData)} since $startData")
        } catch (e: KeeperException.NoNodeException) {
            println("No Master")
        }

        println("Workers: ")
        for (workerStr: String in zkc.getChildren("/workers", false)) {
            val status = zkc.getData("/workers/$workerStr", false, null)
            println("\t$workerStr: ${String(status)}")
        }

        println("Tasks: ")
        for (taskStr: String in zkc.getChildren("/tasks", false)) {
            val command = zkc.getData("/tasks/$taskStr", false, null)
            println("\t$taskStr: ${String(command)}")
        }
    }




}