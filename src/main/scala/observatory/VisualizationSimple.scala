package observatory

import java.util

import com.sksamuel.scrimage
import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 2nd milestone: basic visualization
  */
object VisualizationSimple {
  val p = 2
  val r_km = 6378.14 // 6356.752

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    //    val temps: RDD[(Location, Double)] = sc.parallelize(temperatures.toList)
    predictTemperatureIterable(temperatures, location)
    //    ???
  }

  /**
    * @param colors Pairs containing a value and its associated color
    * @param value  The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(colors: Iterable[(Double, Color)], value: Double): Color = {
    val colorsSorted: java.util.TreeMap[Double, Color] = sortedColors(colors)
    interpolateColor(colorsSorted, value)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    visualize_proposed(temperatures, colors)
    //    ???
  }

  def visualize_proposed(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val locationPixel: Iterable[(Location, Pixel)] = calculatePixels(temperatures, colors)
    val pixels: Iterable[Pixel] = locationPixel.map(x => x._2)
    val img_pixels: Array[Pixel] = pixels.toArray
    Image(361, 181, img_pixels)
  }


  def calculatePixels(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Iterable[(Location, Pixel)] = {
    val colorsSorted: java.util.TreeMap[Double, Color] = sortedColors(colors)

    //get RDD of locations - one location for each pixel of eventual image
    val xaxis = Range(-90, 91)
    val yaxis = Range(-180, 181)
    val points: Iterable[Location] = for (x <- xaxis;
                                          y <- yaxis) yield Location(x, y)

    val location_w_color: List[(Location, Color)] =
      points
        .map(e => (e, predictTemperature(temperatures, e)))
        .map(e => (e._1, interpolateColor(colorsSorted, e._2)))
        .toList
        .sortBy(x => (x._1.lat * -1000) + x._1.lon)

    location_w_color.map(x => (x._1, Pixel(scrimage.Color(x._2.red, x._2.green, x._2.blue))))
  }


  //interpolateColor start
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

  //interpolateColor end

  //predictTemperature start
  def predictTemperatureIterable(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    val dist: (Location) => Double = distance(location)
    val weightAndWeightedTemp: Iterable[(Double, Double)] = temperatures.map(e => (dist(e._1), e._2))
      .map(e => (weight(e._1), e._2))
      .map(e => (e._1, e._1 * e._2))

    val sumOfWeightedTemps = weightAndWeightedTemp.map(e => e._2).sum
    val sumOfWeights = weightAndWeightedTemp.map(e => e._1).sum
    sumOfWeightedTemps / sumOfWeights
  }

  def distance(one: Location)(two: Location): Double = {
    val lonRad_1 = one.lonRad
    val latRad_1 = one.latRad
    val cos_1 = Math.cos(latRad_1)
    val sin1 = Math.sin(latRad_1)

    val latRad_2 = two.latRad
    val cos_2 = Math.cos(latRad_2)
    val sin2 = Math.sin(latRad_2)

    val delta_lon = lonRad_1 - two.lonRad
    val cos_lon = Math.cos(delta_lon)
    val radians = Math.acos((sin1 * sin2) + (cos_1 * cos_2 * cos_lon))
    radians * r_km
  }

  def weight(dist: Double): Double = {
    1 / (Math.pow(dist, p))
  }

  //predictTemperature end

}

