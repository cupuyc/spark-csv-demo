import org.json4s.native.JsonMethods
import org.json4s.{DefaultFormats}
import org.json4s.jackson.Serialization.write
import java.io._

/**
  * Json conversion and filesystem IO
  */
object json {

  implicit val jsonDefaultFormats = DefaultFormats

  def parseRules(rulesRaw: String): Seq[TransformRule] = {

    JsonMethods.parse(rulesRaw)
      .camelizeKeys
      .extract[List[TransformRule]]
  }

  def saveAsJsonFile(output: List[OutResult], fileName: String): Unit = {
    val pw = new PrintWriter(new File(fileName))
    pw.write(write(output))
    pw.close
  }

}
