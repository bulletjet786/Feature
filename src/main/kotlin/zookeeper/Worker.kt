package zookeeper

import org.apache.zookeeper.*
import java.util.*


class Worker(private val connectionString: String): Watcher {

    private val serverId = Integer.toHexString(Random().nextInt())
    private lateinit var zkc: ZooKeeper
    private var status: String = "Init"
    set(status) {
        this.status = status
        this.updateStatus(status)
    }

    override fun process(event: WatchedEvent?) {
        logger.info("$event, $connectionString")
    }

    fun startZK() {
        zkc = ZooKeeper(connectionString, SESSION_TIMEOUT, this)
    }

    fun register() {
        val callback = AsyncCallback.StringCallback { rc, path, ctx, name ->
            when (KeeperException.Code.get(rc)) {
                KeeperException.Code.CONNECTIONLOSS -> register()
                KeeperException.Code.OK -> logger.info("Registered successfully: $serverId")
                KeeperException.Code.NODEEXISTS -> logger.warn("Already registered: $serverId")
                else -> logger.error("Something went wrong in KeeperException.create(Code.get(rc), path): ${KeeperException.Code.get(rc)}")
            }
        }
        zkc.create("/workers/worker-$serverId",
                "Idle".toByteArray(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                callback,
                null)
    }

    fun stop() {
        zkc.close()
    }

    @Synchronized private fun updateStatus(status: String) {
        if (status == this.status) {
            val callback = AsyncCallback.StatCallback { rc, path, ctx, stat ->
                when (KeeperException.Code.get(rc)) {
                    KeeperException.Code.CONNECTIONLOSS -> updateStatus(ctx as String)
                }
            }
            zkc.setData("/workers/worker-$serverId",
                    status.toByteArray(),
                    -1,
                    callback,
                    status)
        }
    }

    fun getWorkers() {
        val watcher = Watcher {
            if (it.type == Watcher.Event.EventType.NodeChildrenChanged) {
                assert("/wrokers" == it.path)
                getWorkers()
            }
        }
        val callback = AsyncCallback.ChildrenCallback { rc, path, ctx, children ->
            when (KeeperException.Code.get(rc)) {
                KeeperException.Code.CONNECTIONLOSS -> {
                    getWorkers()
                }
                KeeperException.Code.OK -> {
                    logger.info("Successfully got a list of workers: ${children!!.size} workers.")
                    reassignAndSet(children)
                }
                else -> {
                    logger.error("getChildren failed with ${KeeperException.Code.get(rc)}!")
                }
            }
        }
        zkc.getChildren("/workers",
                watcher,
                callback,
                null)
    }

    private fun reassignAndSet(children: MutableList<String>) {
        println("zookeeper.Worker.reassignAndSet is not implemented!")       // TODO("not implemented")
    }


}

fun main(args: Array<String>) {
    val worker = Worker("www.bulletjet.cn:2181/application/native")
    worker.startZK()

    worker.register()

    Thread.sleep(60 * 1000)

    worker.stop()
}