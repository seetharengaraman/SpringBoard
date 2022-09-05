This mini project creates random bank transactions and streams using Kafka Producer to Kafka consumer (Generator container). As part of consumption, it is detected if transactions are fraudulent using amount above a specific value. Once determined, it is again streamed through two different topics - one for legitimate transactions and another for fraudulent transactions. Additionally, the transactions are also written to a file. When file size exceeds a certain value, it is sent as an email with attachment to a gmail id (Detector container)


reset.sh is used to clean up docker after container brought down. It is necessary when modifying the container contents else there are situations when the detector and generator containers don't come up with NoBroker error


https://developers.google.com/gmail/api/quickstart/python used to determine method to send email.
