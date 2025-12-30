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
    table-names = ["wiki.mediawiki.page_cdc"]
    #exclude-columns = ["titlevector"]
    #startup-mode = "initial"
    #debezium.slot.drop.on.stop = true
    url = "jdbc:postgresql://db:5432/wiki?loggerLevel=TRACE"
  }
}

sink {
  Console {
    plugin_input = "wiki_cdc"
  }

  Http {
    plugin_input = "wiki_cdc"
    url = "https://abc.feg.com.tw/oauth2/wiki"
  }
}
</pre>

<pre>
CREATE OR REPLACE VIEW mediawiki.page_cdc AS
SELECT
    id,
    title,
    content,
    titlevector::text AS titlevector_text,
    other_column1,
    other_column2
FROM mediawiki.page;  
</pre>
