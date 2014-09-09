import play.Project._

name := "dr-elephant"

version := "0.4"

javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "commons-io" % "commons-io" % "2.4",
  "mysql" % "mysql-connector-java" % "5.1.22",
  "org.apache.commons" % "commons-email" % "1.3.2",
  "org.apache.hadoop" % "hadoop-auth" % "2.3.0",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.7.3",
  "org.jsoup" % "jsoup" % "1.7.3"
)

libraryDependencies ++= (
if(sys.props.get("hadoop.version").exists(_ == "1")) Seq(
  "com.linkedin.li-hadoop" % "hadoop-core" % "1.2.1.45"
)
else if(sys.props.get("hadoop.version").exists(_ == "2")) Seq(
  "com.linkedin.li-hadoop" % "hadoop-common" % "2.3.0.27",
  "com.linkedin.li-hadoop" % "hadoop-mapreduce-client-core" % "2.3.0.27"
)
else Seq()
)

val LinkedInPatterns = Patterns(
      Seq("[organization]/[module]/[revision]/[module]-[revision].ivy"),
      Seq("[organisation]/[module]/[revision]/[artifact]-[revision](-[classifier]).[ext]"),
      isMavenCompatible = true)

val ArtifactoryBaseUrl = "http://artifactory.corp.linkedin.com:8081/artifactory/"

resolvers += Resolver.url("LI repo repository", url(ArtifactoryBaseUrl + "repo"))(LinkedInPatterns)

playJavaSettings
