package curator

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.curator.framework.recipes.leader.LeaderLatchListener
import org.apache.curator.framework.recipes.leader.LeaderSelector
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter
import org.apache.curator.retry.RetryOneTime
import java.util.*


fun main(args: Array<String>) {
    Master("www.bulletjet.cn:2181/application/curator").testLatch()
    Master("www.bulletjet.cn:2181/application/curator").testSelection()
    Master("www.bulletjet.cn:2181/application/curator").testCache()
    Thread.sleep(60 * 1000)
}

class Master(private val connectionString: String) {

    private val id = Integer.toHexString(Random().nextInt())!!

    private fun tryMasterUsingLatch() : String {

        val zkc = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(RetryOneTime(10 * 1000))
                .build()
        zkc.start()

        val listener = object : LeaderLatchListener {
            override fun notLeader() {
                logger.debug("$id is not leader.")
                println("$id is not leader.")
            }

            override fun isLeader() {
                logger.debug("$id is the leader.")
                println("$id is the leader.")
            }
        }

        val leaderLatch = LeaderLatch(zkc, "/master", id)
        leaderLatch.addListener(listener)
        leaderLatch.start()
        logger.debug("leader latch started")
        leaderLatch.await()
        val result = leaderLatch.leader.id
        leaderLatch.close()
        return result
    }


    fun testLatch() {
        val master = Master("www.bulletjet.cn:2181/application/curator")
        println(master.tryMasterUsingLatch())
    }

    private fun tryMasterUsingSelection() {
        val zkc = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(RetryOneTime(10 * 1000))
                .build()
        zkc.start()

        val listener = object : LeaderSelectorListenerAdapter() {
            override fun takeLeadership(client: CuratorFramework?) {
                logger.debug("$id has the leadership.")
                println("$id has the leadership.")
            }
        }

        val leaderSelector = LeaderSelector(zkc, "/leader", listener)
        leaderSelector.start()
        Thread.sleep(5 * 1000)
        leaderSelector.close()
    }

    fun testSelection() {
        tryMasterUsingSelection()
    }

    private fun getWorkers() {
        val zkc = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(RetryOneTime(10 * 1000))
                .build()
        zkc.start()

        val listener = object : PathChildrenCacheListener {
            override fun childEvent(client: CuratorFramework?, event: PathChildrenCacheEvent?) {
                when (event!!.type) {
                    PathChildrenCacheEvent.Type.CHILD_REMOVED -> logger.debug("a worker leave")
                    PathChildrenCacheEvent.Type.CHILD_ADDED -> logger.debug("a worker enter")
                    PathChildrenCacheEvent.Type.CHILD_UPDATED -> logger.debug("a worker change")
                }
            }
        }

        val workers = PathChildrenCache(zkc, "/workers", true)
        workers.listenable.addListener(listener)
        workers.start()
    }

    fun testCache() {
        getWorkers()
    }

}