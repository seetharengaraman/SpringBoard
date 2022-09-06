This mini project creates random bank transactions and streams using Kafka Producer to Kafka consumer (Generator container). As part of consumption, it is detected if transactions are fraudulent using amount above a specific value. Once determined, it is again streamed through two different topics - one for legitimate transactions and another for fraudulent transactions. Additionally, the transactions are also written to a file. When file size exceeds a certain value, it is sent as an email with attachment to a gmail id (Detector container)


reset.sh is used to clean up docker after container brought down. It is necessary when modifying the container contents else there are situations when the detector and generator containers don't come up with NoBroker error


https://developers.google.com/gmail/api/quickstart/python used to determine method to send email.
We need credentials as part of Gmail API enablement (save as credentials.json, sample provided). Then run send_mail.py (with valid from and to email address, from address should be gmail id) once in local to generate token.json. Then move this file along with other files to the docker container before starting using docker-compose up

Start Zookeeper and Kafka broker using:
docker-compose -f docker-compose.kafka.yml up

Subsequently start generator and detector containers using:
docker-compose up
