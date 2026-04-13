import java.net._
import java.io._
import java.util.concurrent._
import scala.collection.concurrent.TrieMap

object ChatServer {

  val clients = TrieMap[String, ClientHandler]()
  val logger = LoggerUtil.logger

  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(9998)
    logger.info("Server started on port 9998")

    val pool = Executors.newCachedThreadPool()

    while (true) {
      val socket = serverSocket.accept()
      pool.execute(new ClientHandler(socket))
    }
  }

  class ClientHandler(socket: Socket) extends Runnable {

    var username: String = _
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val out = new PrintWriter(socket.getOutputStream, true)

    def run(): Unit = {
      try {
        // JOIN message
        val joinMsg = MessageUtil.fromString(in.readLine())
        username = joinMsg.from

        clients.put(username, this)
        logger.info(s"$username joined")

        sendUserListToAll()
        broadcastSystem(s"$username joined the chat")

        var msgStr: String = null

        while ({ msgStr = in.readLine(); msgStr != null }) {
          val msg = MessageUtil.fromString(msgStr)

          msg.msgType match {
            case "BROADCAST" =>
              broadcast(s"[${msg.from}]: ${msg.message}")

            case "PRIVATE" =>
              sendPrivate(msg)

            case "EXIT" =>
              disconnect()

            case _ =>
              logger.warning("Unknown message type")
          }
        }

      } catch {
        case e: Exception =>
          logger.warning(s"Error: ${e.getMessage}")
      } finally {
        disconnect()
      }
    }



    def sendPrivate(msg: Message): Unit = {
      clients.get(msg.to).foreach { client =>
        client.out.println(
          MessageUtil.toString(
            Message("PRIVATE", msg.from, msg.to, msg.message)
          )
        )
      }
    }




    def broadcast(message: String): Unit = {
      clients.values.foreach(_.out.println(
        MessageUtil.toString(
          Message("BROADCAST", "SERVER", "", message)
        )
      ))
    }

    def broadcastSystem(message: String): Unit = {
      clients.values.foreach(_.out.println(
        MessageUtil.toString(
          Message("SYSTEM", "SERVER", "", message)
        )
      ))
    }

    def sendUserListToAll(): Unit = {
      val userList = clients.keys.mkString(",")

      clients.values.foreach(_.out.println(
        MessageUtil.toString(
          Message("LIST", "SERVER", "", userList)
        )
      ))
    }

    def disconnect(): Unit = {
      if (username != null && clients.contains(username)) {
        clients.remove(username)
        logger.info(s"$username disconnected")

        broadcastSystem(s"$username left the chat")
        sendUserListToAll()
      }

      socket.close()
    }
  }
}