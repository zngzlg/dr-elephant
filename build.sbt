//
// Copyright 2015 LinkedIn Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.
//

import play.Project._

name := "dr-elephant"

version := "1.0.6-SNAPSHOT"

javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6")

libraryDependencies ++= Seq(
  javaJdbc,
  javaEbean,
  cache,
  "commons-io" % "commons-io" % "2.4",
  "mysql" % "mysql-connector-java" % "5.1.22",
  "org.apache.hadoop" % "hadoop-auth" % "2.3.0",
  "org.apache.commons" % "commons-email" % "1.3.2",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.7.3",
  "org.jsoup" % "jsoup" % "1.7.3",
  // The following two dependencies are pulled in by the metrics library.
  "org.apache.avro" % "avro" % "1.4.0",
  "com.linkedin.avro-schemas" % "avro-schemas-tracking" % "6.0.518",
  // Spark dependencies, exclude avro transitive dependencies to avoid being overriden by 1.7.x
  "org.apache.spark" % "spark-core_2.10" % "1.4.0" excludeAll(
        ExclusionRule(organization = "org.apache.avro"),
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "net.razorvine")
      ),
  // Hadoop defaultly are using guava 11.0, might raise NoSuchMethodException
  "com.google.guava" % "guava" % "18.0",
  "com.google.code.gson" % "gson" % "2.2.4"
)

ivyConfigurations += config("compileonly").hide

unmanagedClasspath in Compile ++= update.value.select(configurationFilter("compileonly"))

libraryDependencies ++= Seq(
  "com.linkedin.hadoop" % "hadoop-common" % "2.3.0.+" % "compileonly",
  "com.linkedin.hadoop" % "hadoop-hdfs" % "2.3.0.+" % "compileonly",
  "com.linkedin.hadoop" % "hadoop-common" % "2.3.0.+" % "test",
  "com.linkedin.hadoop" % "hadoop-hdfs" % "2.3.0.+" % "test"
)

val LinkedInPatterns = Patterns(
      Seq("[organization]/[module]/[revision]/[module]-[revision].ivy"),
      Seq("[organisation]/[module]/[revision]/[artifact]-[revision](-[classifier]).[ext]"),
      isMavenCompatible = true)

val ArtifactoryBaseUrl = "http://artifactory.corp.linkedin.com:8081/artifactory/"

resolvers += Resolver.url("LI repo repository", url(ArtifactoryBaseUrl + "release"))(LinkedInPatterns)

playJavaSettings