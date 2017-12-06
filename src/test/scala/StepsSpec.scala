import org.specs2._

object StepsSpec extends mutable.Specification {

  "Spark task" should {
    "load, filter and transform" in {

      val result = MainApp.process("src/main/resources/sample.csv", "src/main/resources/rules.json")
      println(result)
      result.size === 3
    }
  }
}