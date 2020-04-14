ruleset shop {
    meta {
        provides driver_rankings
        shares driver_rankings
    }

    global {
        driver_rankings = function() {
            ent:driver_rankings.defaultsTo({})
        }
    }

    rule claim_delivery {
        select when delivery claimed

        pre {
            driver_id = event:attr("driver_id")
        }

        if (ent:driver_rankings.defaultsTo({}){driver_id}) then
        event:send({
            "eci": event:attr("return_eci"),
            "eid": "selected-driver",
            "domain": "delivery_driver",
            "type": "selected",
            "attrs": {
                "selected_eci": event:attr("return_eci")
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

        always {
            raise driver_ranking event "updated"
                attributes event:attrs
        }
    }
}