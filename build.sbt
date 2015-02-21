name := "S3RDD"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core" % "1.3.0-SNAPSHOT" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  //"com.amazonaws" % "aws-java-sdk" % "1.9.17",
  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test"
)

assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set(
    "ant-1.9.0.jar",
    "netty-3.6.6.Final.jar",
    "py4j-0.8.1.jar",
    "jets3t-0.7.1.jar",
    "hadoop-client-2.6.0.jar",
    "hadoop-core-2.6.0.jar",
    "spark-core_2.10-1.0.0.jar",
    "scala-compiler.jar",
    "jsp-api-2.1-6.1.14.jar",
    "jsp-2.1-6.1.14.jar",
    "jasper-compiler-5.5.12.jar",
    "javax.activation-1.1.0.v201105071233.jar",
    "javax.mail.glassfish-1.4.1.v201005082020.jar",
    "javax.servlet-3.0.0.v201112011016.jar",
    "jetty-xml-8.1.14.v20131031.jar",
    "jetty-webapp-8.1.14.v20131031.jar",
    "jetty-util-8.1.14.v20131031.jar",
    "javax.transaction-1.1.1.v201105210645.jar",
    "jetty-continuation-8.1.14.v20131031.jar",
    "jetty-http-8.1.14.v20131031.jar",
    "jetty-io-8.1.14.v20131031.jar",
    "jetty-jndi-8.1.14.v20131031.jar",
    "jetty-plus-8.1.14.v20131031.jar",
    "jetty-security-8.1.14.v20131031.jar",
    "jetty-server-8.1.14.v20131031.jar",
    "jetty-servlet-8.1.14.v20131031.jar",
    "jcl-over-slf4j-1.7.5.jar",
    "zookeeper-3.4.5.jar",
    "hadoop-yarn-api-2.4.0.jar",
    "mesos-0.18.1.jar",
    "hadoop-yarn-client-2.4.0.jar",
    "hadoop-yarn-common-2.4.0.jar",
    "hadoop-yarn-server-common-2.4.0.jar",
    "minlog-1.2.jar", // Otherwise causes conflicts with Kyro
    "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
    "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
    "commons-beanutils-1.7.0.jar",
    "stax-api-1.0.1.jar",
    "asm-3.1.jar",
    "scalatest-2.0.jar",
    "mockito-all.jar"
  )
  cp filter { jar => excludes(jar.data.getName) }
}

//mergeStrategy in assembly <<= (mergeStrategy in assembly) {
//  (old) => {
//    case "project.clj" => MergeStrategy.discard // Leiningen build files
//    case x => old(x)
//  }
//}