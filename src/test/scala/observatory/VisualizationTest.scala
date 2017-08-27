package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {


  val a = (10D, Color(0, 0, 0))
  val b = (110D, Color(100, 100, 100))
  val tempsColors = Iterable(a, b)
  val colors = Visualization.sortedColors(tempsColors)


  test("confirm color")(
    assert(midColor(60) == Color(50,50,50))
  )

  test("confirm color out of range over")(
    assert(midColor(150) == Color(100,100,100))
  )

  test("confirm color out of range under")(
    assert(midColor(0) == Color(0,0,0))
  )

  private def midColor(v:Double) = {
    Visualization.interpolateColor(tempsColors,v)
  }


}
