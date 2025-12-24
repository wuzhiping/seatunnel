# seatunnel
https://seatunnel.apache.org/docs/start-v2/docker/

./bin/seatunnel.sh -DJvmOption="-Xms4G -Xmx4G" -m local -c ./config/v2.streaming.conf.template

docker run --rm -it --network mediawiki_default  -p 15031:8080 apache/seatunnel bash

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

# postgresql CDC
* https://seatunnel.apache.org/zh-CN/docs/connector-v2/source/PostgreSQL-CDC/
* https://www.postgresql.org/docs/current/runtime-config-replication.html
* https://blog.csdn.net/gitblog_00090/article/details/151701324
