package observatory

import java.util

import com.sksamuel.scrimage
import com.sksamuel.scrimage.{Image, Pixel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  @transient lazy val sparkSession = SparkSession.builder.
    master("local")
    .config("spark.driver.memory", "100mb")
    .appName("spark session example")
    .getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  val sc = sparkSession.sparkContext
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
    //    visualize_proposed(temperatures, colors)
    ???
  }

  def visualize_proposed(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val locationPixel: RDD[(Location, Pixel)] = calculatePixels(temperatures, colors)
    val pixels: RDD[Pixel] = locationPixel.map(x => x._2)
    val img_pixels: Array[Pixel] = pixels.collect()
    Image(361, 181, img_pixels)
  }

  def calculatePixels_old(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): RDD[(Location, Pixel)] = {
    val temps: RDD[(Location, Double)] = sc.parallelize(temperatures.toList)
    val colorsSorted: java.util.TreeMap[Double, Color] = sortedColors(colors)

    //get RDD of locations - one location for each pixel of eventual image
    val xaxis = sc.parallelize(Range(-90, 91))
    val yaxis = sc.parallelize(Range(-180, 181))
    val points: RDD[(Int, Int)] = xaxis.cartesian(yaxis)
    val locations: RDD[Location] = points.map(x => Location(x._1, x._2))

    //locations with temp
    //    val locations_x_temps: RDD[(Location, (Location, Double))] = locations.cartesian(temps)
    //    val distAndTemps: RDD[(Location, (Double, Double))] = locations_x_temps.map(e => (e._1, locationDistanceAndTemp(e._1, e._2)))
    //    val tempAndWeight: RDD[(Location, (Double, Double))] = distAndTemps.map(e => (e._1, locationTempWeight(e._2)))
    //    val weightedTempAndWeight: RDD[(Location, (Double, Double))] = tempAndWeight.map(e => (e._1, locationWeightAndWeightedTemp( e._2)))

    val weightedTempAndWeight = locations.cartesian(temps).map(e => from_location_and_temp_to_weighted_temp_and_weight(e))

    val locations_w_top_bottom: RDD[(Location, (Double, Double))] = weightedTempAndWeight.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val locations_w_temp: RDD[(Location, Double)] = locations_w_top_bottom.map(x => (x._1, x._2._1 / x._2._2))
    val sorted_locations_w_temp: RDD[(Location, Double)] = locations_w_temp.sortBy(x => (x._1.lat * -1000) + x._1.lon)
    val location_w_color: RDD[(Location, Color)] = sorted_locations_w_temp.map(e => (e._1, interpolateColor(colors, e._2)))

    location_w_color.map(x => (x._1, Pixel(scrimage.Color(x._2.red, x._2.green, x._2.blue))))
  }

  def calculatePixels(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): RDD[(Location, Pixel)] = {
    val colorsSorted: java.util.TreeMap[Double, Color] = sortedColors(colors)

    //get RDD of locations - one location for each pixel of eventual image
    val xaxis = sc.parallelize(Range(-90, 91))
    val yaxis = sc.parallelize(Range(-180, 181))
    val points: RDD[(Int, Int)] = xaxis.cartesian(yaxis)
    val locations: RDD[Location] = points.map(x => Location(x._1, x._2))

    val location_w_color: RDD[(Location, Color)] = locations.map(e => (e, predictTemperature(temperatures, e)))
      .map(e => (e._1, interpolateColor(colorsSorted, e._2)))
      .sortBy(x => (x._1.lat * -1000) + x._1.lon)

    location_w_color.map(x => (x._1, Pixel(scrimage.Color(x._2.red, x._2.green, x._2.blue))))
  }

  def from_location_and_temp_to_weighted_temp_and_weight(entry: (Location, (Location, Double))): (Location, (Double, Double)) = {
    val location = entry._1
    (location, locationWeightAndWeightedTemp(locationTempWeight(locationDistanceAndTemp(location, entry._2))))
  }

  def locationDistanceAndTemp(location: Location, locationTemp: (Location, Double)): (Double, Double) = {
    val l2 = locationTemp._1
    val t = locationTemp._2

    (distance(location)(l2), t)
  }

  def locationTempWeight(distanceTemp: (Double, Double)): (Double, Double) = {
    val distance = distanceTemp._1
    val t = distanceTemp._2

    (t, weight(distance))
  }

  def locationWeightAndWeightedTemp(tempWeight: (Double, Double)): (Double, Double) = {
    val t = tempWeight._1
    val weight = tempWeight._2

    (t * weight, weight)
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
  def predictTemperature(temperatures: RDD[(Location, Double)], location: Location): Double = {
    val dist: (Location) => Double = distance(location)
    val distAndTemps: RDD[(Double, Double)] = temperatures.map(e => (dist(e._1), e._2))
    val weightAndTemp: RDD[(Double, Double)] = distAndTemps.map(e => (weight(e._1), e._2))
    val weightAndWeightedTemp: RDD[(Double, Double)] = weightAndTemp.map(e => (e._1, e._1 * e._2))

    val sumOfWeightedTemps = weightAndWeightedTemp.map(e => e._2).sum
    val sumOfWeights = weightAndWeightedTemp.map(e => e._1).sum
    sumOfWeightedTemps / sumOfWeights
  }

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

