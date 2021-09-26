# nacta-door-lock-sync

## install
   pip3 install -r requirement.txt -i https://pypi.mirrors.ustc.edu.cn/simple/ 

## confluent-kafka

    download [confluent-kafka](http://confluent.io)
    decompress confluent
    add mysql jdbc driver to confluent/share/java/kafka-connect-jdbc

## start

    startup zookeeper,kafka-server,schema-registry,kafka-connect,connector

- bin/zookeeper-server-start etc/kafka/zookeeper.properties
- bin/kafka-server-start etc/kafka/server.properties
- bin/schema-registry-start ${workspace}/eurasia-attendance-kafka/etc/schema-registry/schema-registry.properties
- bin/connect-standalone ${workspace}/eurasia-attendance-kafka/etc/schema-registry/connect-avro-standalone.properties ${workspace}/eurasia-attendance-kafka/etc/kafka-connect-jdbc/source-mysql-roomis.properties


## debug

    cd /opt/apps/confluent/confluent-3.3.2/bin

    ./kafka-console-consumer  --bootstrap-server localhost:9092 --topic roomis-vw_attendance_report


## got it 

  . if you want reconsume message,change the value "kafka.group_id" 
