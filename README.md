# seatunnel
https://seatunnel.apache.org/docs/start-v2/docker/

./bin/seatunnel.sh -DJvmOption="-Xms4G -Xmx4G" -m local -c ./config/v2.streaming.conf.template

docker run --rm -it --network mediawiki_default  -p 15031:8080 apache/seatunnel bash

mv /opt/seatunnel/lib/opengauss-jdbc-5.1.0.jar /opt/seatunnel/lib/opengauss-jdbc-5.1.0.jar.bak

# 对于 SeaTunnel Zeta 引擎
您需要确保 jdbc 驱动 jar 包 已放置在目录 ${SEATUNNEL_HOME}/lib/ 中。
请下载并将 PostgreSQL 驱动放入 ${SEATUNNEL_HOME}/lib/ 目录。例如：cp postgresql-xxx.jar $SEATUNNEL_HOME/lib/

## 以下是启用 PostgreSQL 中的 CDC（变化数据捕获）的步骤：
* 确保 wal_level 设置为 logical：通过在 postgresql.conf 配置文件中添加 "wal_level = logical" 来修改，重启 PostgreSQL 服务器以使更改生效。 或者，您可以使用 SQL 命令直接修改配置：
<pre>
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
</pre>
* 将指定表的 REPLICA 策略更改为 FULL
<pre>
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
</pre>
* wiki example
<pre>
psql -U wikiuser -d wiki
\l
\dn
\dt wiki.*
\d+ wiki.page

ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
#restart pg
ALTER TABLE wiki.page REPLICA IDENTITY FULL;
</pre>

# publication
<pre>
SELECT * FROM pg_publication;
DROP PUBLICATION dbz_publication;
CREATE PUBLICATION dbz_publication FOR  TABLE wiki.mediawiki.orders;
 ALTER PUBLICATION dbz_publication ADD  TABLE wiki.mediawiki.job;
 ALTER PUBLICATION dbz_publication DROP TABLE wiki.mediawiki.job;


SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'dbz_publication'
ORDER BY schemaname, tablename;
</pre>

# kafka broker
<pre>

  broker:
    image: apache/kafka:4.0.0
    container_name: broker
    hostname: broker
    volumes:
      - ./kafka:/tmp/kraft-combined-logs
    ports:
      - 9092:9092
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:9093

      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3

  
  /opt/kafka/bin
./kafka-topics.sh --list --bootstrap-server broker:9092
./kafka-console-consumer.sh --bootstrap-server broker:9092 --topic seatunnel_prod --from-beginning
  
</pre>

# postgresql CDC
* https://seatunnel.apache.org/zh-CN/docs/connector-v2/source/PostgreSQL-CDC/
* https://www.postgresql.org/docs/current/runtime-config-replication.html
* https://blog.csdn.net/gitblog_00090/article/details/151701324
<pre>
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Postgres-CDC {
    plugin_output = "wiki_cdc"
    username = "wikiuser"
    password = "wikipass"
    database-names = ["wiki"]
    schema-names = ["mediawiki"]
    table-names = ["wiki.mediawiki.orders"]
    url = "jdbc:postgresql://db:5432/wiki?loggerLevel=TRACE"

    # 关键修改：避免每次从头 snapshot
    startup.mode = "earliest"

  }
}

transform {
  Metadata {
    plugin_input = "wiki_cdc"
    metadata_fields {
        Database = _database_
        Table = _table_
        RowKind = _rowKind_
        EventTime = _ts_ms_
        Delay = _delay_
    }
    plugin_output = "wiki_cdc_meta"
  }
}

sink {
  Console {
    plugin_input = "wiki_cdc"
  }

  Http {
    plugin_input = "wiki_cdc_meta"
    url = "https://abc.feg.com.tw/oauth2/wiki"
  }

  Kafka {
    plugin_input = "wiki_cdc_meta"
    bootstrap.servers = "broker:9092"               # 多节点用逗号分隔
    topic = "seatunnel_prod"
    format = json
    semantic = exactly-once

    kafka.config = {
      acks = "all"                                  # 确保消息被所有副本确认
      enable.idempotence = true                      # 幂等生产
      retries = 10                                   # 重试次数
      max.in.flight.requests.per.connection = 5     # 保证事务安全
      linger.ms = 20                                 # 批量发送优化
    }
  }
  
}

</pre>

# [API](https://seatunnel.apache.org/zh-CN/docs/2.3.12/seatunnel-engine/rest-api-v2/)
* submit-job
<pre>
import requests
import json

url = f"http://10.17.1.26:15060/submit-job?jobId=&jobName=FAKE"

payload = {
    "env": {
        "parallelism": 2,
        "job.mode": "STREAMING",
        "checkpoint.interval": 10000
    },
    "source": [
        {
            "plugin_name": "FakeSource",
            "plugin_output": "fake",
            "row.num": 10,
            "schema": {
                "fields": {
                    "name": "string",
                    "age": "int",
                    "card": "int"
                }
            }
        }
    ],
    "transform": [],
    "sink": [
        {
            "plugin_name": "Console",
            "plugin_input": ["fake"]
        }
    ]
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

print("Status Code:", response.status_code)
print("Response Body:", response.text)

</pre>

* stop-job
<pre>
jobId = response.json()["jobId"]
jobId = "1058644825752469505"
import requests
import json

url = "http://10.17.1.26:15060/stop-job"

payload = {
    "jobId": jobId
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, data=json.dumps(payload))

print("Status Code:", response.status_code)
print("Response Body:", response.text)

</pre>
