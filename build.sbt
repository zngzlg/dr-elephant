import play.Project._

name := "dr-elephant"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache
)

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-auth" % "2.3.0"

libraryDependencies += "org.jsoup" % "jsoup" % "1.7.3"

playJavaSettings
