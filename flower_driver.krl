ruleset flower_driver {
  meta {
    name "Flower Delivery Driver"
    author "Jeremy Rees"
    logging on

    shares share_gossip, process_rumor, compare_notes, change_lat_long, request_payment, __testing, getRumors
    use module io.picolabs.subscription alias Subs
    use module io.picolabs.wrangler alias wrangler

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
    __testing = { 	"queries": [
      {"name": "getRumors", "args":[]}
    ],
    "events": [ 
                { "domain": "delivery", "type": "completed", "attrs": [ "Delivery" ] },
                { "domain": "driver_gossip H", "type": "heartbeat", "attrs": [] },
                { "domain": "shop", "type": "subscription_wanted", "attrs": ["eci"] }
            ] 
    }

    getRumors = function() {
      ent:rumors
    }

    getDeliveries = function() {
      ent:deliveries.defaultsTo({})
    }
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
      schedule driver_gossip event "heartbeat" at time:add(time:now(), {"seconds": 1})
    }
  }

  rule share_gossip {
    select when driver_gossip heartbeat

    pre {
      // get a peer
      peers = Subs:established("Tx_role", "gossip_node").klog("Possible Gossipers")
      ind = random:integer(peers.length()-1)
      peer = peers[ind].klog("Selected peer: ")

      // prepare a message
      event = {
        "eci": peer{"Tx"},
        "domain": "driver_gossip",
        "type": "seen",
        "attrs": {
          "seen": ent:last_seen.klog("LAST SEEN"),
          "returnChannel": peer{"Rx"}
        }
      }
    }

    event:send(event)

    always {
      // re-schedule the next heartbeat
      schedule driver_gossip event "heartbeat" at time:add(time:now(), {"seconds": 1})
    }
  }

  rule process_rumor {
    select when gossip rumor
      foreach event:attrs{"rumors"} setting (rumor)

    pre {
      // get the data from the message
      originId = rumor{"MessageID"}.klog("MessageID for rumor").substr(0, 25)
    }

    // if this rumor is one we haven't seen before, it's available, and it's close enough, try to claim it
    if (ent:rumors.keys().none(function(x) {x == originId}) || ent:rumors{originId}.keys().klog("SequenceNumbers").none(function(x) {x == rumor{"SequenceNumber"}})).klog("IsUnique")
        && (rumor{"Delivery"}{"Status"} == "available").klog("IsAvailable")
        && (google_maps:distance(ent:lat, ent:long, rumor{"Delivery"}{"Latitude"}, rumor{"Delivery"}{"Longitude"}).klog("Distance Calculation") < ent:distance_threshold).klog("IsWithinDistance")
        then
      event:send({
        "eci": rumor{"Delivery"}{"Shop_ECI"},
        "domain": "delivery",
        "type": "claimed",
        "attrs": {
          "driver_id": meta:picoId,
          "rumor": rumor
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
    select when driver_gossip seen
      foreach ent:rumors setting (rumors, originId)
        foreach rumors setting (rumor, sequenceNumber)

    if (event:attrs{"seen"}.none(function(x) {originId == x}) || event:attrs{"seen"}{originId} < sequenceNumber) then
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
      delivery = event:attrs{"Delivery"}.klog("Completed Delivery")
    }

    event:send({
      "eci": delivery,
      "domain": "delivery",
      "type": "delivered",
      "attrs": {
        "driver_id": meta:picoId,
        "paypal_id": ent:paypal_id
      }
    })
  }

  rule add_driver_subscription {
    select when shop subscription_wanted

    always {
      raise wrangler event "subscription" attributes {
        "name": "gossip",
        "wellKnown_Tx": event:attr("eci"),
        "Rx_role":"gossip_node",
        "Tx_role":"gossip_node"
      }
    }
  }

  rule auto_accept {
	  select when wrangler inbound_pending_subscription_added
	  fired {
	  	Tx_role = event:attr("wellKnown_Tx").klog("wellKnown_Tx")
		raise wrangler event "pending_subscription_approval"
		  attributes event:attrs
	  }
	}

}
