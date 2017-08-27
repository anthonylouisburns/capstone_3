package observatory


import com.sksamuel.scrimage.{Image, Pixel}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  val a = (10D, Color(0, 0, 0))
  val b = (110D, Color(100, 100, 100))
  val tempsColors = Iterable(a, b)
  val colors = VisualizationSimple.sortedColors(tempsColors)
  def temps: Iterable[(Location, Double)] = List((Location(89.0, 0.0), 0.0),(Location(-89.0, 0.0), 100.0))


//  test("confirm color")(
//    assert(midColor(60) == Color(50,50,50))
//  )
//
//  test("confirm color out of range over")(
//    assert(midColor(150) == Color(100,100,100))
//  )
//
//  test("confirm color out of range under")(
//    assert(midColor(0) == Color(0,0,0))
//  )
//
//
  test("0x0")({
    val t = VisualizationSimple.predictTemperature(temps, Location(0, 0))
    assert(t === 50)
  })
//
//  test("0x0 dist to 89.0, 0.0")({
//    assert(Visualization.distance(Location(0, 0))(Location(89.0, 0.0)) === 9907.439340630452)
//    assert(Visualization.distance(Location(0, 0))(Location(-89.0, 0.0)) === 9907.439340630452)
//  })
//
  test("calculatePixels")({
    val c:Iterable[(Double, Color)] = Iterable((100D, Color(100, 100, 100)),(0D, Color(0, 0, 0)))
    def t:Iterable[(Location, Double)] = List((Location(89.0, 0.0), 0.0),(Location(-89.0, 0.0), 100.0))

    val pixels = VisualizationSimple.calculatePixels(t, c)
    val p:Pixel = pixels.filter(x=>x._1 == Location(0,0)).head._2
    assert(p.red === 50D)
  })

  test("img")({
    val c:Iterable[(Double, Color)] = Iterable((100D, Color(255, 0, 0)),(0D, Color(0, 0, 255)))
    def t:Iterable[(Location, Double)] = List((
      Location(89.0, 0.0), 0.0),
      (Location(-89.0, 0.0), 0.0),
      (Location(0.0, 0.0), 100.0),
      (Location(0.0, 90.0), 100.0),
      (Location(0.0, -90.0), 100.0),
      (Location(0.0, 180.0), 100.0))

    val img:Image = VisualizationSimple.visualize(t, c)
    out(img)
  })
  def out(img:Image): Unit ={
    img.output("/Users/aburns/src/capstone/observatory_3/some-image.png")
  }
//
//  def printPixel(p:Pixel): Unit ={
//    println(s"red:${p.red} blue:${p.blue} green:${p.green}")
//  }
//  private def midColor(v:Double) = {
//    Visualization.interpolateColor(tempsColors,v)
//  }
//
//
//  def pt():Unit={
//    val v =Visualization.predictTemperature(temps, Location(-90.0,-180.0))
//    println(v)
//  }
}
