ruleset shop {
    meta {
        provides driver_rankings
        shares driver_rankings, __testing

        use module io.picolabs.subscription alias Subscriptions
        use module io.picolabs.wrangler alias wrangler

        use module shop_keys
        use module paypal
            with access_token = keys:paypal{"access_token"}
    }

    global {
        latitude = 0
        longitude = 0

        driver_rankings = function() {
            ent:driver_rankings.defaultsTo({})
        }

        __testing = { 	"queries": [],
        "events": [ 
                    { "domain": "delivery", "type": "start", "attrs": [] },
                    { "domain": "shop", "type": "subscription_wanted", "attrs": ["eci"] }
                ] 
        }
    }

    rule init {
        select when wrangler ruleset_added where event:attr("rids") >< meta:rid
        if ent:last_seen.isnull() then noop();
        fired {
            ent:sequence_number := 0
        }
    }
    

    rule start_delivery_request {
        select when delivery start
        foreach Subscriptions:established("Tx_role", "driver").klog("delivery subs") setting (subscription)

        event:send({
            "eci": subscription{"Tx"},
            "eid": "request-delivery",
            "domain": "gossip",
            "type": "rumor",
            "attrs": {
                "MessageId": meta:picoId + ":" + ent:sequence_number.defaultsTo(0),
                "SequenceNumber": ent:sequence_number.defaultsTo(0),
                "Delivery": {
                    "Shop_ECI": subscription{"Rx"},
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Status": "available"
                }
            }
        })

        fired {
            ent:sequence_number := ent:sequence_number.defaultsTo(0) + 1
        }
    }

    rule claim_delivery {
        select when delivery claimed
        foreach Subscriptions:established("Tx_role", "driver") setting (subscription)
        
        pre {
            driver_id = event:attr("driver_id")
        }

        if (ent:threshold.defaultsTo(0) <= ent:driver_rankings.defaultsTo({}){driver_id}) then
        event:send({
            "eci": subscription{"Tx"},
            "eid": "selected-driver",
            "domain": "delivery_driver",
            "type": "selected",
            "attrs": {
                "MessageId": meta:picoId + ":" + ent:sequence_number.defaultsTo(0),
                "SequenceNumber": ent:sequence_number.defaultsTo(0),
                "Delivery": {
                    "Shop_ECI": subscription{"Rx"},
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Status": "claimed"
                }
            }
        })
    }

    rule update_ranking {
        select when driver_ranking updated

        always {
            driver_id = event:attr("driver_id")
            ent:driver_rankings := ent:driver_rankings.defaultsTo({}).put([driver_id], ent:driver_rankings.defaultsTo({}){"driver_id"}.defaultsTo(0)+1)
        }
    }

    rule delivery_delivered {
        select when delivery delivered
        pre {
            paypal_id = event:attr("paypal_id")
        }

        always {
            raise driver_ranking event "updated"
                attributes event:attrs

            _ = paypal:send_payment(paypal_id, ent:paypal_item_id.defaultsTo(0))

            ent:paypal_item_id := ent:paypal_item_id.defaultsTo(0) + 1
        }
    }

    rule add_driver_subscription {
        select when shop subscription_wanted

        always {
			raise wrangler event "subscription" attributes {
			  "name": "driver",
			  "wellKnown_Tx": event:attr("eci"),
			  "Rx_role":"shop",
			  "Tx_role":"driver",
            }
		}
    }
}