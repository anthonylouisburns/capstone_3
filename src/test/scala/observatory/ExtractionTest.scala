package observatory

import java.time.LocalDate

import observatory.Extraction.{YEARS, dfYear}
import org.apache.spark.sql._
import org.scalatest.FunSuite

trait ExtractionTest extends FunSuite {
  test("stations file ")({
    assert(Extraction.getClass.getResource("/1976.csv").getPath.equals("/Users/aburns/src/capstone/observatory_3/target/scala-2.11/classes/1976.csv"))
  })

  test("show stations")({
    assert(cnt(Extraction.dfStations(stationFile())) == 28131)
  })

  test("1975")({
    assert(cnt(Extraction.dfYear(yearFile(1975), 1975)) == 2190974)
  })

  test("1975 resource")({
    assert(cnt(Extraction.dfYear("/1975.csv", 1975)) == 2190974)
  })

  test("temp correct at location")({
    val x = Extraction.locateTemps(1975, "/stations.csv", "/1975.csv")
    val l = Location(+70.933,-008.667)

    x.filter((a:Observation)=>a.location.lat == +70.933).show()
  })


  //  test("years")({
  //    assert(cnt(dfYears()) == 111062262)
  //  })

  test("ds show stations")({
    Extraction.dsStations(stationFile()).show(5)
  })

  test("ds show 1975")({
    Extraction.dsYear(yearFile(1975), 1975).show(5)
  })

  test("locateTemps")({
    Extraction.locateTemps(1975, stationFile(), yearFile(1975)).show(5)
  })

  test("resource")({
    println(stationFile())
  })

  test("locationYearlyAverageRecords"){
    val x = List((LocalDate.now(), Location(0,0), 0.0))
    println(Extraction.locationYearlyAverageRecords(x))
  }

  test("locateTemperatures"){
    println(Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv"))
  }

  test("50f is 10c"){
    assert(Extraction.celsius(50) == 10)
  }

  def cnt(df: DataFrame): Long = {
    df.show(5)
    val c = df.count()
    println(c)
    c
  }

  def dfYears(): DataFrame = {
    YEARS.map(y => dfYear(yearFile(y), y)).reduce((l: DataFrame, r: DataFrame) => l.union(r))
  }

  def yearFile(year: Int) = {
    getClass.getResource(s"/$year.csv").getPath
  }

  def stationFile()= {
    getClass.getResource("/stations.csv").getPath
  }
}