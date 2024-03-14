spark-submit \
--class org.ics.isl.DAP \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.speculation=true \
--master local[*] \
./target/diaeresis-1.0.jar \
mydataset69 \
1 \
hdfs://localhost:9000 \
/home/masuda/uba1.7/University0_0.nt \
/home/masuda/uba1.7/schemaLUBM100_1300_2300b.txt
