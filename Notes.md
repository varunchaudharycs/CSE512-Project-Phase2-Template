# Setup

- Add `.master("local[*]")` to SparkSQLExample main method for the SparkSession
- To run in IDE(IntelliJ), change apache spark dependencies to `compile` instead of the default `provided`.
- Install Java 8, by following `https://www.fosstechnix.com/install-oracle-java-8-on-ubuntu-20-04/`
- Install sbt `1.4.2`
- Install spark(for spark-submit) `2.4.1`

# Execution

- navigate to project dir
- `sbt assembly`
- `spark-submit target/scala-2.11/CSE512-Project-Phase2-Template-assembly-0.1.0.jar result/output rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1`
- Match `exampleanswer` counts to `result/output` saved files