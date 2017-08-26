package observatory

// stations.csv  STN identifier	WBAN identifier	Latitude	Longitude
case class StationIdentifier(stn:String, wban:String)
case class Station(stationIdentifier: StationIdentifier, location: Location)
case class StationRaw(stn:String, wban:String, lat: Double, lon: Double){
  def station():Station={
    Station(StationIdentifier(stn,wban), Location(lat, lon))
  }
}

case class ObservationRaw(stn:String, wban:String, month:Int, day:Int, temperature: Double, year:Year){
  def observation():ObservationStation={
    ObservationStation(StationIdentifier(stn,wban), YMD(year,month,day), temperature)
  }
}

// STN identifier	WBAN identifier	Month	Day	Temperature (in degrees Fahrenheit)
case class ObservationStation(stationIdentifier: StationIdentifier, ymd: YMD, temperature: Temperature)

object Observation{
  def apply(station: Station, observationStation: ObservationStation):Observation = {
    this(station.location, observationStation.ymd, observationStation.temperature)
  }
}
case class YMD(year:Int, month:Int, day:Int)

case class Observation(location: Location, ymd: YMD, temperature: Temperature)