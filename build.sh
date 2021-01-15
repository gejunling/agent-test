#!/bin/bash

mvn clean install -DskipTests

rm -rf agent-test
mkdir agent-test

cp -r target/lib agent-test
cp -r ./src/main/resources/conf agent-test
cp data.sql agent-test


chmod +x start.sh
cp start.sh agent-test


tar zcf agent-test.tgz agent-test

scp agent-test/lib/agent-test-1.0-SNAPSHOT.jar root@orcl-ali:/home/fzsge/agent-test/lib
#scp agent-test/lib/* root@orcl-ali:/home/fzsge/agent-test/lib
