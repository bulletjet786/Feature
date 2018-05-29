### ZooKeeper native API
#　Connection
1. 当客户端连接上服务器后，客户端会收到SyncConnected事件；当客户端从服务器上断开连接后，客户端会收到Disconnected事件。
2. 当客户端非正常断开连接，会自己尝试重连客户端。
3. 当服务器在Session_Timeout时间内都没有收到客户端的心跳消息后，服务器定义该Session失效，当客户端重新连接上去后，会收到Expired事件。

