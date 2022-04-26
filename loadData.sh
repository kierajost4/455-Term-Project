$HADOOP_HOME/bin/hadoop fs -rm -r /TP
$HADOOP_HOME/bin/hadoop fs -mkdir /TP
$HADOOP_HOME/bin/hadoop fs -mkdir /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/child_care_centers.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/hospitals.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/local_law_enforcement_locations.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/places_of_worship.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/private_schools.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/institutions/public_schools.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/county_population.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/crime_data.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -put data/gisjoin_data/cleaned_meta_data.csv /TP/data
$HADOOP_HOME/bin/hadoop fs -ls /TP/data