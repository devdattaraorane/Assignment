import java.net._
import java.io._
import scala.io.StdIn

object ChatClient {

  def main(args: Array[String]): Unit = {

    val socket = new Socket("localhost", 9998)

    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val out = new PrintWriter(socket.getOutputStream, true)

    println("Enter username:")
    val username = StdIn.readLine()

    // JOIN
    val joinMsg = Message("JOIN", username, "", "")
    out.println(MessageUtil.toString(joinMsg))

    
    
    // Receiver Thread
    val receiver = new Thread(() => {
      var msg: String = null

      while ({ msg = in.readLine(); msg != null }) {
        val parsed = MessageUtil.fromString(msg)

        parsed.msgType match {
          case "BROADCAST" =>
            println(parsed.message)

          case "PRIVATE" =>
            println(s"[PRIVATE][${parsed.from}]: ${parsed.message}")

          case "SYSTEM" =>
            println(s"[SYSTEM]: ${parsed.message}")

          case "LIST" =>
            println(s"[ACTIVE USERS]: ${parsed.message}")

          case _ =>
            println(msg)
        }
      }
    })

    receiver.start()

    // Sender Loop
    while (true) {
      val input = StdIn.readLine()

      if (input.equalsIgnoreCase("exit")) {
        val exitMsg = Message("EXIT", username, "", "")
        out.println(MessageUtil.toString(exitMsg))
        socket.close()
        System.exit(0)
      }

      if (input.startsWith("@")) {
        val parts = input.split(" ", 2)
        val toUser = parts(0).substring(1)
        val message = parts(1)

        val privateMsg = Message("PRIVATE", username, toUser, message)
        out.println(MessageUtil.toString(privateMsg))

      } else {
        val broadcastMsg = Message("BROADCAST", username, "", input)
        out.println(MessageUtil.toString(broadcastMsg))
      }
    }
  }
}