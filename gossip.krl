ruleset gossip {
    meta {
        use module io.picolabs.subscription alias Subscriptions
        use module io.picolabs.wrangler alias wrangler
        use module shop

        shares __testing, getRumors, createSeenMessage, getPeers
        provides getRumors, createSeenMessage, getPeers
    }

    global {
        __testing = { 	
            "queries": [
                {"name": "getRumors", "args":[]},
                {"name": "createSeenMessage", "args":[]},
                {"name": "getPeers", "args": []}
            ],
            "events": [
                { "domain": "gossip", "type": "subscription_wanted", "attrs": ["gossip_id", "eci"] },
                { "domain": "gossip", "type": "interval", "attrs": ["interval"] }
            ] 
        }

        getRumors = function() {
            ent:rumors.defaultsTo({})
        }

        getPeers = function() {
            ent:peers.defaultsTo({})
        }

        interval = 5

        getMessageID = function() {
            meta:picoId + ":" + (ent:sequenceVariable.defaultsTo(0))
        }

        hasNewRumor = function() {
            temps = shop:driver_rankings()
            my_nums = ent:rumors.defaultsTo({})

            my_nums = my_nums{meta:picoId}.defaultsTo({}).keys()

            return (temps.length().klog("hasNewRumor-length") > my_nums.length().klog("hasNewRumor-prev_length")).klog("hasNewRumor-result")
        }

        createRumorMessage = function() {
            temps = shop:driver_rankings()
            latest_temp = temps[temps.length()-1]
            act_temp = latest_temp{"temperature"}
            time = latest_temp{"timestamp"}

            return {
                "MessageID": getMessageID().klog("createRumorMessage-MessageID"),
                "SensorID": meta:picoId,
                "Temperature": act_temp,
                "Timestamp": time,
            }
        }

        createSeenMessage = function() {
            ent:rumors.defaultsTo({}).map(function(num_map, picoID) {
                sorted_nums = num_map.keys().klog("createSeenMessage-keys").map(function(a) {
                    a.as("Number")
                }).sort(function(a, b) { a < b  => -1 |
                    a == b =>  0 |
                               1
                }).klog("createSeenMessage-sorted_nums")
                
                sorted_nums.filter(function(x) {x == sorted_nums.index(x)})[sorted_nums.length()-1].defaultsTo(-1)
            })
        }

        getPeer = function() {
            //peers is a map of maps which has each subscription id maping to that subscription's last seen

            peers = ent:peers.defaultsTo({}).klog("getPeer-peers")
            

            my_rumors = ent:rumors.defaultsTo({})

            //find any peers which have a message less than what I have for that message

            greater_count = peers.filter(function(messageIDs, peerID) {
                messageIDs.filter(function(highest_seen, messageID) {
                    highest_seen < my_rumors{messageID}
                }).length() > 0
            })
            
            return (greater_count.length() > 0) =>
                greater_count.keys()[0] |
                peers.keys()[0]
        }

        prepareMessage = function(subscriber) {
            //choose a message type to send (seen or rumor)
            type = random:integer(1)

            type = (hasNewRumor()) => random:integer(1) | 1

            type = (type == 1)

            //format the message
            return (type) =>
                [{
                    "eci": subscriber, "eid": "heartbeat-seen",
                    "domain": "gossip", "type": "seen",
                    "attrs": {
                        "eci": subscriber,
                        "seen": createSeenMessage()
                    }
                }, type] |
                [{
                    "eci": subscriber, "eid": "heartbeat-rumor",
                    "domain": "gossip", "type": "rumor",
                    "attrs": {
                        "eci": subscriber,
                        "rumor": createRumorMessage().klog("prepareMessage-rumor-message")
                    }
                }, type]
        }
    }

    rule seen_gossip {
        select when gossip seen where ent:process.defaultsTo("on") == "on"

        pre {
            my_rumors = ent:rumors.defaultsTo({})
            seen_rumors = event:attr("seen")
            subscriptionID = Subscriptions:established("Rx", event:attr("eci").klog("search_sub-eci"))[0]{"Tx"}.klog("seen_gossip-sub")
            peers = ent:peers.defaultsTo({}).put([subscriptionID], seen_rumors)

            //find the difference between the rumors
            diff = my_rumors.map(function(num_map, picoID) {
                picoID = picoID.klog("gossip_seen-picoID")
                num_map.filter(function(rumor, num) {
                    num.as("Number") > seen_rumors{picoID}.defaultsTo(-1)
                }).values().klog("gossip_seen-innermap")
            }).klog("gossip_seen-mapdiff").values().filter(function(m) {
                m.length() > 0
            }).reduce(function(a,b) {
                a.append(b)
            }, []).klog("gossip_seen-diff")
        }

        if (diff.length() > 0) then
            noop()
        
        fired {
            ent:peers := ent:peers.defaultsTo({}).put([subscriptionID], seen_rumors)

            raise gossip event "send_diff" 
                attributes {"diff": diff, "return_eci": subscriptionID}

        } else {
            ent:peers := ent:peers.defaultsTo({}).put([subscriptionID], seen_rumors)
        }
    }

    rule send_diff_gossip {
        select when gossip send_diff
        foreach event:attr("diff") setting (d)

        event:send({
            "eci": event:attr("return_eci"),
            "eid": "diff-rumor",
            "domain": "gossip",
            "type": "rumor",
            "attrs": {
                "eci": event:attr("return_eci"),
                "rumor": d
            }
        })
    }

    rule rumor_gossip {
        select when gossip rumor where ent:process.defaultsTo("on") == "on"

        pre {
            new_rumor = event:attr("rumor").klog("rumor_gossip-rumor")
            messageID = new_rumor{"MessageID"}
            picoID = messageID.split(re#:#)[0]
            num = messageID.split(re#:#)[1]
        }

        fired {
            ent:rumors := ent:rumors.defaultsTo({}).put([picoID, num], new_rumor)
        }
    }

    rule gossip {
        select when gossip heartbeat where ent:process.defaultsTo("on") == "on"

        pre {
            subscriber = getPeer().klog("subscriber")
            m_and_type = prepareMessage(subscriber).klog("message_and_type")
            m = m_and_type[0].klog("gossip-m")
            type = m_and_type[1].klog("type")
        }

        if (not subscriber.isnull()) then
            event:send(m)
        
        always {
            messageID = m{"attrs"}{"rumor"}{"MessageID"}
            picoID = messageID.split(re#:#)[0]
            num = messageID.split(re#:#)[1].klog("num")
            rumor = m{"attrs"}{"rumor"}.klog("rumor")

            ent:rumors := (type) => ent:rumors.defaultsTo({}) | ent:rumors.defaultsTo({}).put([meta:picoId, num], rumor)
            ent:sequenceVariable := (type) => ent:sequenceVariable.defaultsTo(0) | ent:sequenceVariable.defaultsTo(0) + 1

            raise gossip event "check_subs" attributes{}

            schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": interval}).klog("scheduled")
        }
    }

    rule check_subs {
        select when gossip check_subs
        foreach Subscriptions:established("Tx_role", "node").klog("node-subs") setting (subscription)

        always {
            ent:peers := ent:peers.defaultsTo({}).put([subscription{"Tx"}], ent:peers{subscription{"Tx"}}.defaultsTo({})).klog("check_subs-peers")
        }
    }

    rule add_gossip_subscription {
        select when gossip subscription_wanted

        always {
			raise wrangler event "subscription" attributes {
			  "name": "gossipers",
			  "wellKnown_Tx": event:attr("eci"),
			  "Rx_role":"node",
			  "Tx_role":"node",
            }
		}
    }

    rule start_gossip {
        select when wrangler ruleset_added where rids >< meta:rid.klog("meta rid")
        
        always {

            schedule gossip event "heartbeat" at time:add(time:now(), {"seconds": ent:interval.defaultsTo(interval)}).klog("scheduled")
        }
    }

    rule set_gossip_interval {
        select when gossip interval

        always {
            ent:interval := event:attr("interval").defaultsTo(interval).as("Number")
        }
    }

    rule switch_gossip {
        select when gossip process

        always {
            prev = ent:process.defaultsTo("on")
            ent:process := event:attr("process")
        }
    }
}