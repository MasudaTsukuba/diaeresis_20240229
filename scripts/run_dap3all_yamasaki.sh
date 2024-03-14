spark-submit \
--class org.ics.isl.DAP \
--driver-memory 32g \
--executor-memory 32g \
--conf spark.speculation=true \
--master local[*] \
./target/diaeresis-1.0.jar \
yamasaki001 \
4 \
hdfs://localhost:9000 \
/home/masuda/uba1.7/all_yamasaki.nt \
/home/masuda/uba1.7/schemaLUBM100_1300_2300b.txt
