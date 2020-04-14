ruleset google_maps {
  meta {
    configure using maps_api_key = ""
    provides distance
  }
 
  global {
    distance = function(from_lat, from_long, to_lat, to_long) {
      // returns number of seconds from origin to destination
      base_url = <<https://maps.googleapis.com/maps/api/distancematrix/json>>
      query = <<?units=imperial&origins=#{from_lat},#{from_long}&destinations=#{to_lat},#{to_long}&key=#{maps_api_key}>>
      resp = http:get(base_url + query){"content"}.decode()

      resp{"rows"}[0]{"elements"}[0]{"duration"}{"value"}
    }
  }
}