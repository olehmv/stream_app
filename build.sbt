name := "stream_app"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"          % sparkVersion,
  "org.apache.spark" %% "spark-sql"           % sparkVersion,
  "org.apache.spark" %% "spark-mllib"         % sparkVersion,
  "org.apache.spark" %% "spark-streaming"     % sparkVersion,
  "org.apache.spark" %% "spark-hive"          % sparkVersion,
  "org.scalafx"      %% "scalafx"             % "8.0.144-R12",
  "org.scalactic"    %% "scalactic"           % "3.0.5",
  "org.scalatest"    %% "scalatest"           % "3.0.5" % "test",
  "com.holdenkarau"  %% "spark-testing-base"  % "1.5.1_0.2.1" % Test,
  "mysql"            % "mysql-connector-java" % "5.1.6",
  "org.apache.kafka" %% "kafka" % "0.10.0.1",
  "io.spray" %%  "spray-json" % "1.3.4"
)
