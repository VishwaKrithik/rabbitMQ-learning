Run node consumer.js
Run node producer.js to send dummy requests to queue
It runs until successful if not sends to retry queue which has dead letter exchange
If it fails thrice it is sent to DLQ