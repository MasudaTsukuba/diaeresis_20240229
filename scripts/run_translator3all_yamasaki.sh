spark-submit \
--class org.ics.isl.QueryTranslator \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.speculation=true \
--master local[*] \
./target/diaeresis-1.0.jar \
yamasaki001 \
4 \
hdfs://localhost:9000 \
./queries/lubm100_1300_2300B

