spark-submit \
--class org.ics.isl.QueryProcessor \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.speculation=true \
--master local[*] \
./target/diaeresis-1.0.jar \
mydataset67 \
4 \
hdfs://localhost:9000 \
./queries/lubm100_1300_2300B/translated_queries_4_bal

