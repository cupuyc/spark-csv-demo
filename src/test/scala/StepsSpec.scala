import org.specs2._

object StepsSpec extends mutable.Specification {

  "Spark task" should {
    "load, filter and transform" in {

      val result = MainApp.process("src/test/resources/sample.csv", "src/test/resources/rules.json")
      println()
      println(result)
      println()

      result.size === 3
    }
  }
}