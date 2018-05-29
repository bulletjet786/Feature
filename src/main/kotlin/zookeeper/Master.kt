package zookeeper

import org.apache.zookeeper.*
import org.apache.zookeeper.data.Stat
import java.util.*

enum class MasterStatus {
    ELECTED, NOT_ELECTED, RUNNING
}

fun main(args: Array<String>) {

    val master = Master("www.bulletjet.cn:2181/application/native")
    master.connect()
    master.changeToMasterAsync()


    Thread.sleep(20 * 1000)

    if (master.isLeader) {
        master.bootstrap()
        Thread.sleep(1200 * 1000)
    } else {
        println("Someone else is the leader!")
    }

    master.stop()

}

class Master(private val connectionString: String) : Watcher {

    lateinit var zkc: ZooKeeper
    private val serverId = Integer.toHexString(Random().nextInt())
    var isLeader = false

    fun connect() {
        zkc = ZooKeeper(connectionString, SESSION_TIMEOUT, this)
    }

    override fun process(event: WatchedEvent?) {
        println(event)
    }

    fun stop() {
        zkc.close()
    }

    /**********************************************************************************************/
    /**
     * 同步调用，尝试切换至主节点模式
     */
    fun changeToMasterSync() {
        while (true) {
            try {
                zkc.create("/master",
                        serverId.toByteArray(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL)
                isLeader = true
                break
            } catch (e: KeeperException.NodeExistsException) {
                isLeader = false
                break
            } catch (e: KeeperException.ConnectionLossException) {
                if (checkMasterSync()) break
            }
        }

    }

    private fun checkMasterSync(): Boolean {
        while (true) {
            try {
                var stat = Stat()
                val data = zkc.getData("/master", false, stat)
                isLeader = String(data) == (serverId)
                return true
            } catch (e: KeeperException.NoNodeException) {
                return false
            } catch (e: KeeperException.ConnectionLossException) {
            }
        }
    }

    /**********************************************************************************************/
    private var state = MasterStatus.NOT_ELECTED

    /**
     * 异步调用，尝试切换至主节点模式
     */
    fun changeToMasterAsync() {
        logger.debug("Create Node /master !")
        val callback = object : AsyncCallback.StringCallback {
            override fun processResult(rc: Int, path: String?, ctx: Any?, name: String?) {
                when (KeeperException.Code.get(rc)) {
                    KeeperException.Code.CONNECTIONLOSS -> {
                        logger.debug("Connection loss!")
                        checkMasterAsync()
                        return
                    }
                    KeeperException.Code.OK -> {
                        logger.debug("Code is OK!")
                        this@Master.state = MasterStatus.ELECTED
                        isLeader = true
                        takeLeadership()
                        return
                    }
                    KeeperException.Code.NODEEXISTS -> {
                        state = MasterStatus.NOT_ELECTED
                        masterExists()
                    }
                }
            }
        }
        zkc.create("/master",
                serverId.toByteArray(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                callback,
                null)
    }

    private fun masterExists() {
        val watcher = Watcher {
            if (it.type == Watcher.Event.EventType.NodeDeleted) {
                assert("/master" == it.path)
                changeToMasterAsync()
            }
        }
        val callback = AsyncCallback.StatCallback { rc, path, ctx, stat ->
            when (KeeperException.Code.get(rc)) {
                KeeperException.Code.CONNECTIONLOSS -> {
                    masterExists()
                    return@StatCallback
                }
                KeeperException.Code.OK -> {
                    if (state == null) {
                        state = MasterStatus.RUNNING
                        changeToMasterAsync()
                    }
                    return@StatCallback
                }
                else -> checkMasterAsync()
            }
        }
        zkc.exists("/master",
                watcher,
                callback,
                null)

    }

    private fun takeLeadership() {
        println("Take Leadership!")
    }

    private fun checkMasterAsync() {
        val callback = AsyncCallback.DataCallback { rc, path, ctx, data, stat ->
            when (KeeperException.Code.get(rc)) {
                KeeperException.Code.CONNECTIONLOSS -> {
                    checkMasterAsync()
                    return@DataCallback
                }
                KeeperException.Code.NONODE -> {
                    changeToMasterAsync()
                    return@DataCallback
                }
                else -> logger.debug("something expecting in checkMasterAsync with KeeperException.Code.get(rc)")
            }
        }
        zkc.getData("/master",
                false,
                callback,
                null)
    }

    fun bootstrap() {
        createParent("/workers", ByteArray(0))
        createParent("/assign", ByteArray(0))
        createParent("/tasks", ByteArray(0))
        createParent("/status", ByteArray(0))
    }

    private fun createParent(path: String, data: ByteArray) {
        val callback = object : AsyncCallback.StringCallback {
            override fun processResult(rc: Int, path: String?, ctx: Any?, name: String?) {
                when (KeeperException.Code.get(rc)) {
                    KeeperException.Code.CONNECTIONLOSS -> createParent(path!!, ctx as ByteArray)
                    KeeperException.Code.NODEEXISTS -> logger.warn("Parent already registered:$path")
                    KeeperException.Code.OK -> logger.info("Parent created")
                    else -> logger.error("Something went error: in KeeperException.create(KeeperException.Code.get(rc), path)")
                }
            }
        }
        zkc.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                callback,
                data)
    }
}
