# seatunnel
https://seatunnel.apache.org/docs/start-v2/docker/

./bin/seatunnel.sh -DJvmOption="-Xms4G -Xmx4G" -m local -c ./config/v2.streaming.conf.template

docker run --rm -it --network mediawiki_default  -p 15031:8080 apache/seatunnel bash

mv /opt/seatunnel/lib/opengauss-jdbc-5.1.0.jar /opt/seatunnel/lib/opengauss-jdbc-5.1.0.jar.bak

![etl](https://github.com/user-attachments/assets/60ac41b0-c39e-4511-8d5e-1998f0cdf7e0)

![sbpb](https://github.com/user-attachments/assets/2bbd8d80-14d9-4edc-864a-f98884443d77)

# dataease
* https://dataease.io/desktop/index.html
  
# Flink 2.2.1
* https://nightlies.apache.org/flink/flink-docs-release-2.2/zh/docs/dev/python/overview/
* https://seatunnel.apache.org/zh-CN/docs/2.3.12/start-v2/locally/quick-start-flink/
* https://seatunnel.apache.org/zh-CN/docs/2.3.12/other-engine/flink
* https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/#session-cluster-sql-yaml
* https://pyflink.readthedocs.io/en/main/getting_started/index.html
<pre>
  kafka-ui:
    container_name: kafka-ui
    image: provectuslabs/kafka-ui:latest
    ports:
      - 29092:8080
    environment:
      DYNAMIC_CONFIG_ENABLED: 'true'
    #volumes:
    #  - ~/kui/config.yml:/etc/kafkaui/dynamic_config.yaml

  zookeeper:
    image: zookeeper:3.9.2
    hostname: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOO_TICK_TIME: 2000
    #volumes:
    #  - ./zk-data:/data
    #  - ./zk-logs:/logs

  zkui:
    image: tobilg/zookeeper-webui
    ports:
      - "22181:8080"
    environment:
      ZK_DEFAULT_NODE: "zookeeper:2181/"
      USER: admin
      PASSWORD: admin
    depends_on:
      - zookeeper
  
  jobmanager:
    image: shawoo/pyflink:2.2.1
    ports:
      - "28081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: shawoo/pyflink:2.2.1
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 5
        # 关键：增加总内存到至少 8GB
        taskmanager.memory.process.size: 8g
        # 您的堆外内存配置
        taskmanager.memory.task.off-heap.size: 4g
        # 可选：优化其他内存
        taskmanager.memory.managed.size: 512m
        taskmanager.memory.network.fraction: 0.05

  pyflink:
    image: shawoo/pyflink:2.2.1
    depends_on:
      - jobmanager
    ports:
      - "28082:8888"
    command: bash -c "cd examples/python && jupyter lab --allow-root --ip=0.0.0.0 --NotebookApp.token='12345678'"
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2

  sql-client:
    image: shawoo/pyflink:2.2.0
    command: bin/sql-client.sh
    depends_on:
      - jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager    



  ./bin/flink run -m 10.17.1.26:28081 -py ./examples/python/datastream/word_count.py
</pre>


# 对于 SeaTunnel Zeta 引擎
您需要确保 jdbc 驱动 jar 包 已放置在目录 ${SEATUNNEL_HOME}/lib/ 中。
请下载并将 PostgreSQL 驱动放入 ${SEATUNNEL_HOME}/lib/ 目录。例如：cp postgresql-xxx.jar $SEATUNNEL_HOME/lib/

## 以下是启用 PostgreSQL 中的 CDC（变化数据捕获）的步骤：
* 确保 wal_level 设置为 logical：通过在 postgresql.conf 配置文件中添加 "wal_level = logical" 来修改，重启 PostgreSQL 服务器以使更改生效。 或者，您可以使用 SQL 命令直接修改配置：
<pre>
#------------------------------------------------------------------------------
# WRITE-AHEAD LOG
#------------------------------------------------------------------------------
# - Settings -
wal_level = logical                     # minimal, replica, or logical
</pre>
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

# 逻辑复制
<pre>
前提,pg1 FDB     pg2 TDB有同样的tb1，tb2表结构
    无主键，或者serial类型的 tb2需要  ALTER TABLE public.tb2 REPLICA IDENTITY FULL;
 
发布
create publication my_pub for table public.tb1;
-- 加表
alter publication my_pub add table public.tb2;
-- 减表
alter publication my_pub drop table tb2;
-- 删除
drop publication if exists my_pub;
 
订阅
create subscription my_sub_slot connection 'host=pg1 port=5432 dbname=FDB user=postgres password=postgres' publication my_pub
-- 查看订阅进度
SELECT * FROM pg_stat_subscription;
</pre>

# REPLICA slot
<pre>
 查看磁盘占用
 select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_insert_lsn(),restart_lsn)) 
       AS wal_delay ,
       * from pg_catalog.pg_replication_slots ;

 删除
 select * from pg_drop_replication_slot('my_slot');
</pre>

# publication 由 DBA 创建 publication，CDC 用户只负责订阅。
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
    
    # 已验证
    # https://seatunnel.apache.org/docs/2.3.12/connector-v2/source/PostgreSQL-CDC
    slot.name = "seatunnel"
    debezium = {
      "publication.autocreate.mode":"filtered"
      "publication.name" = "dbz_publication"
    }
    #
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

* config vs json
<pre>
 env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 20000
}

source {
  Postgres-CDC {
    plugin_output = "fake"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["public"]
    table-names = ["postgres_cdc.public.orders"]
    url = "jdbc:postgresql://10.17.1.22:5432/postgres_cdc"
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '___') as name, age+1 as age from xxx"
  }
}

sink {
  Redis {
    plugin_input = "fake1"
    host = 10.17.1.22
    port = 6379
    key = "pg_cdc:{id}"
    data_type = key
    support_custom_key = true
  }
}
</pre>
<pre>
curl -X POST "http://10.17.1.26:15060/submit-job" \
  -H "Content-Type: application/json" \
  -d '{
    "env": {
      "parallelism": 2,
      "job.mode": "STREAMING",
      "checkpoint.interval": 20000
    },
    "source": [
        {
          "plugin_name": "Postgres-CDC",
          "plugin_output": "fake",
          "username": "postgres",
          "password": "postgres",
          "database-names": ["postgres_cdc"],
          "schema-names": ["public"],
          "table-names": ["postgres_cdc.public.orders"],
          "url": "jdbc:postgresql://10.17.1.22:5432/postgres_cdc"
        }
    ],
    "transform": [
        {
          "plugin_name": "Sql",
          "plugin_input": "fake",
          "plugin_output": "fake1",
          "query": "select id, name, age+11 as age from xxx"
        }
    ],
    "sink": [
        {
          "plugin_name": "Redis",
          "plugin_input": "fake1",
          "host": "10.17.1.22",
          "port": 6379,
          "key": "pg_cdc:{id}",
          "data_type": "key",
          "support_custom_key": true
        }
    ]
}'
</pre>

# MySQL CDC
* https://seatunnel.apache.org/zh-CN/docs/2.3.12/connector-v2/source/MySQL-CDC
* mysql.conf
<pre>
# Enable binary replication log and set the prefix, expiration, and log format.
# The prefix is arbitrary, expiration can be short for integration tests but would
# be longer on a production system. Row-level info is required for ingest to work.
# Server ID is required, but this will vary on production systems
server-id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 10
binlog_format     = row
# mysql 5.6+ requires binlog_row_image to be set to FULL
binlog_row_image  = FULL

# optional enable gtid mode
# mysql 5.6+ requires gtid_mode to be set to ON, but not required by mysql 8.0+
gtid_mode = on
enforce_gtid_consistency = on


mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| Variable_name            | Value          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | ON             |
| gtid_mode                | ON             |
| log_bin                  | ON             |
+--------------------------+----------------+    
</pre>
<pre>
source {
  MySQL-CDC {
    url = "jdbc:mysql://10.17.1.26:3306/demo"
    username = "root"
    password = "root"
    table-names = ["demo.user"]

    startup.mode = "initial"
  }
}
</pre>

# Doris Sink
* https://doris.apache.org/zh-CN/docs/4.x/gettingStarted/quick-start/
<pre>
show backends;

create database demo;

use demo; 

-- 为当前数据库设置默认副本数为 1（立即生效，无需重启）
ALTER DATABASE demo SET PROPERTIES ("replication_allocation" = "tag.location.default:1");
    
-- 查看数据库的默认副本配置
SHOW CREATE DATABASE demo;
-- 查看表的副本数
SHOW CREATE TABLE mytable;
-- 查看 Backend 节点状态
SHOW PROC '/backends';
    
sink {
  Doris {
    fenodes = "10.17.1.26:8040"
    username = root
    password = ""
    database = "demo"
    table = "${table_name}_test"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
</pre>
* 外部Catalog
<pre>
SHOW CATALOGS;

CREATE CATALOG jdbc_mysql_demo PROPERTIES (
   "type" = "jdbc",
   "user" = "root",
   "password" = "root",
   "jdbc_url" = "jdbc:mysql://10.17.1.22:3306/demo",
   "driver_url" = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar",
   "driver_class" = "com.mysql.cj.jdbc.Driver"
);

switch jdbc_mysql_demo;

show databases;
</pre>
* be jdbc_drivers
<pre>
be bash
/opt/apache-doris/be/jdbc_drivers
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
</pre>


# MongoDB CDC
* [https://seatunnel.apache.org/zh-CN/docs/2.3.12/connector-v2/source/MySQL-CDC](https://seatunnel.apache.org/docs/connector-v2/source/MongoDB-CDC)
* mongod --replSet rs0 --bind_ip_all
* mongosh --eval "rs.initiate()"
<pre>
source {
  MongoDB-CDC {
    hosts = "10.17.1.22:27017"
    database = ["demo"]
    collection = ["demo.abc"]
    schema = {
      table = "demo.abc"
      fields {
        "_id" : string,
        "name" : string,
        "age" : int
      }
    }
  }
} 
</pre>
