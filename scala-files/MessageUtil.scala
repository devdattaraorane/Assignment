case class Message
(
  msgType: String, from: String, to: String, message: String
)

object MessageUtil {

  def toString(msg: Message): String = {
    s"${msg.msgType}|${msg.from}|${msg.to}|${msg.message}"
  }

//  def fromString(str: String): Message = {
//    val parts = str.split("\\|", 4)
//
//    Message(
//      parts(0),
//      parts(1),
//      parts(2),
//      parts(3)
//    )
//  }

  def fromString(str: String): Message = {
    val parts = str.split("\\|", 4)

    if (parts.length < 4) {
      throw new IllegalArgumentException("Invalid message format")
    }

    Message(parts(0), parts(1), parts(2), parts(3))
  }
}