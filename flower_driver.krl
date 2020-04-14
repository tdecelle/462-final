ruleset flower_driver {
  meta {
    name "Flower Delivery Driver"
    author "Jeremy Rees"
    logging on

    shares share_gossip, process_rumor, compare_notes, change_lat_long, request_payment
    use module io.picolabs.subscription alias Subs

    use module google_maps_key
    use module google_maps
        with maps_api_key = keys:google_maps{"maps_api_key"}
  }


//  data in rumor: correlation id, sequence number, delivery object
//  data in delivery object: shop ECI, lat, long, status (available or claimed)

// Shop-Driver interface details:
// shop needs to initially notify of a delivery's existence with event structured as a rumor (so that we get their sequence number)
//

  global {
  }

  rule init {
    select when wrangler ruleset_added where event:attr("rids") >< meta:rid
    if ent:last_seen.isnull() then noop();
    fired {
      ent:last_seen := {}
      ent:rumors := {}
      ent:latitude := 70.199263
      ent:longitude := -148.459178
      ent:paypal_id := 42
      ent:distance_threshold := 900
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": 1})
    }
  }

  rule share_gossip {
    select when gossip heartbeat

    pre {
      // get a peer
      peers = Subs:established("Tx_role", "gossip_node")
      ind = random:integer(peers.length()-1)
      peer = peers[ind].klog("Selected peer: ")

      // prepare a message
      event = {
        "eci": peer{"Tx"},
        "domain": "gossip",
        "type": "seen",
        "attrs": {
          "seen": ent:last_seen,
          "returnChannel": peer{"Rx"}
        }
      }
    }

    event:send(evnt)

    always {
      // re-schedule the next heartbeat
      schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": 1})
    }
  }

  rule process_rumor {
    select when gossip rumor
      foreach event:attrs{"rumors"} setting (rumor)

    pre {
      // get the data from the message
      originId = rumor{"MessageID"}.substr(0, 25)
    }

    // if this rumor is one we haven't seen before, it's available, and it's close enough, try to claim it
    if (not originId >< ent:rumors || not rumor{"SequenceNumber"} >< ent:rumors{originId})
        && rumor{"Delivery"}{"Status"} == "available"
        && maps.distance(ent:lat, ent:long, rumor{"Delivery"}{"Latitude"}, rumor{"Delivery"}{"Longitude"}) < ent:distance_threshold
        then
      event:send({
        "eci": rumor{"Delivery"}{"Shop_ECI"},
        "domain": "delivery",
        "type": "claimed",
        "attrs": {
          "driver_id": meta:picoId
        }
      })

    always {
      // record the rumor in our entity variable
      ent:rumors := ent:rumors.put([originId, rumor{"SequenceNumber"}], rumor)

      // maybe update our last_seen entry
      ent:last_seen{originId} := ent:rumors{originId}
            .keys()
            .sort("ciremun")
            .reduce(function(curr_max, elem, idx, arr) {
              arr[idx-1] == elem + 1 => curr_max | elem
            })
    }
  }

  rule compare_notes {
    select when gossip seen
      foreach ent:rumors setting (rumors, originId)
        foreach rumors setting (rumor, sequenceNumber)

    if (not event:attrs{"seen"} >< originId) || event:attrs{"seen"}{originId} < sequenceNumber then
      event:send({
        "eci": event:attrs{"returnChannel"},
        "domain": "gossip",
        "type": "rumor",
        "attrs": {
          "rumors": [rumor]
        }
      })
  }

  rule change_lat_long {
    select when delivery lat_long_update

    pre {
      lat = event:attrs{"lat"}.as("Number")
      long = event:attrs{"long"}.as("Number")
    }

    always {
      ent:latitude := lat
      ent:longitude := long
    }
  }

  rule request_payment {
    select when delivery completed

    pre {
      delivery = event:attrs{"Delivery"}
    }

    event:send({    // TODO what does the shop ruleset want in this event?
      "eci": delivery{"Shop_ECI"},
      "domain": "delivery",
      "type": "delivered",
      "attrs": {
        "driver_id": meta:picoId,
        "paypal_id": ent:paypal_id
      }
    })
  }

}
