Scala Spark based application for data ingestion pipeline, which does data wrangling and data cleaning and 
writes successful data into 'successful' folder and writes discarded unsuccessful records into 'unsuccessful' folder



command to run on cluster:

./bin/spark-submit \
--master <master-url> \
--deploy-mode <deploy-mode> \
--conf <key<=<value> \
--driver-memory <value>g \
--executor-memory <value>g \
--executor-cores <number of cores>  \
--jars  <comma separated dependencies>
--class <main-class> \
<application-jar> \
[application-arguments]



cron pattern to run the job:

0 * * * * .<path to spark>/bin/spark-submit 