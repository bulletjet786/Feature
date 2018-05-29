package curator

import org.slf4j.LoggerFactory
import zookeeper.Master

val logger = LoggerFactory.getLogger(Master::class.java)
const val SESSION_TIMEOUT: Int = 15 * 1000
