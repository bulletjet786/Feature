package zookeeper
import org.slf4j.LoggerFactory

val logger = LoggerFactory.getLogger(Master::class.java)!!
const val SESSION_TIMEOUT: Int = 15 * 1000
