import java.util.logging._

object LoggerUtil {
  val logger: Logger = Logger.getLogger("ChatAppLogger")

  val fileHandler = new FileHandler("chat.log", true)
  fileHandler.setFormatter(new SimpleFormatter())

  logger.addHandler(fileHandler)
  logger.setUseParentHandlers(false)
}
