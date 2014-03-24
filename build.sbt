import play.Project._
import scala.collection.mutable

name := "dr-elephant"

version := "0.1-SNAPSHOT"

javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "commons-io" % "commons-io" % "2.4",
  "org.apache.hadoop" % "hadoop-auth" % "2.3.0",
  "org.jsoup" % "jsoup" % "1.7.3",
  "mysql" % "mysql-connector-java" % "5.1.22"
)

playJavaSettings