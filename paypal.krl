ruleset paypal {
    meta {
      configure using access_token = ""
      provides send_payment
      shares send_payment
    }
   
    global {
        send_payment = function(receiver_id, paypal_item_id) {
            base_url = <<https://api.sandbox.paypal.com/v1/payments/payouts/>>

            http:post(base_url, json = {
                    "items": [
                        {
                            "recipient_type": "PAYPAL_ID",
                            "amount": {
                                "value": "5.00",
                                "currency": "USD"
                            },
                            "sender_item_id": paypal_item_id,
                            "receiver": receiver_id
                        }
                    ]
                },
                headers = {
                    "Authorization": "Bearer " + access_token
                }
            )
        }
    }
  }