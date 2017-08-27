package observatory

import java.util

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{after, before, before_after}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("GlobalWarming")
    .set("spark.ui.port", "3080")
    .set("spark.driver.bindAddress", "127.0.0.1")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val p = 2
  val r_km = 6378.14 // 6356.752
  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    ???
  }

  /**
    * @param tempColors Pairs containing a value and its associated color
    * @param value      The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(tempColors: Iterable[(Double, Color)], value: Double): Color = {
    val colors = sortedColors(tempColors)
    interpolateColor(colors, value)
  }

  def interpolateColor(colors: java.util.TreeMap[Double, Color], value: Double): Color = {
    val b = Option(colors.lowerKey(value)).getOrElse(colors.firstKey())
    val a = Option(colors.higherKey(value)).getOrElse(colors.lastKey())

    if (a == b) {
      colors.get(a)
    } else {
      avgColor(colors.get(b), colors.get(a), b, a, value)
    }
  }

  def sortedColors(tempColors: Iterable[(Double, Color)]): java.util.TreeMap[Double, Color] = {
    val tm = new util.TreeMap[Double, Color]()
    tempColors.foreach(dc => {
      tm.put(dc._1, dc._2)
    })
    tm
  }

  def avgColor(bcolor: Color, acolor: Color, b: Double, a: Double, d: Double): Color = {
    val aweight = (d - b) / (a - b)
    val bweight = 1 - aweight
    Color(Math.round((bcolor.red * bweight) + (acolor.red * aweight)).toInt,
      Math.round((bcolor.green * bweight) + (acolor.green * aweight)).toInt,
      Math.round((bcolor.blue * bweight) + (acolor.blue * aweight)).toInt)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    ???
  }

  def before_after(points: Iterable[(Double, Color)], value: Double): ((Double, Color), (Double, Color)) = {
    (before(points.head, points.tail, value), after(points.head, points.tail, value))
  }

  def after(a: (Double, Color), points: Iterable[(Double, Color)], value: Double): (Double, Color) = points match {
    case Nil => a
    case h :: t => {
      if (a._1 > value) {
        if (h._1 < a._1 && h._1 > value) after(h, t, value)
        else after(a, t, value)
      } else {
        if (h._1 > a._1) after(h, t, value)
        else after(a, t, value)
      }
    }
  }

  def before(a: (Double, Color), points: Iterable[(Double, Color)], value: Double): (Double, Color) = points match {
    case Nil => a
    case h :: t => {
      if (a._1 < value) {
        if (h._1 > a._1 && h._1 < value) before(h, t, value)
        else before(a, t, value)
      } else {
        if (h._1 < a._1) before(h, t, value)
        else before(a, t, value)
      }
    }
  }
}

