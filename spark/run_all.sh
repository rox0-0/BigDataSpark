#!/bin/bash
set -e
export PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"

echo " Запускаем лабу"
SPARK="/opt/spark/bin/spark-submit"

echo " ETL ->PostgreSQL..."
$SPARK \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  --packages org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark/work-dir/etl_to_pg.py


echo " Ждём готовности ClickHouse..."
for i in {1..30}; do
  if curl -s http://lab2_clickhouse:8123/ping | grep -q "Ok"; then
    echo " ClickHouse готов"
    break
  fi
  echo "  ван мор трай ($i/30)..."
  sleep 1
done

echo " Отчёты -> ClickHouse..."
$SPARK \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/clickhouse-jdbc-0.5.0-http.jar \
  --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.5.0,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark/work-dir/reports_to_ch.py

echo " ГООООЛ! Все джобы выполнены!"