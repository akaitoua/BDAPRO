name := "data-integration-ms"
 
version := "1.0" 
      
lazy val `data-integration-ms` = (project in file(".")).enablePlugins(PlayScala)

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"
      
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
      
scalaVersion := "2.12.2"

libraryDependencies ++= Seq( jdbc , ehcache , ws , specs2 % Test , guice )

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )

libraryDependencies += "org.webjars" % "bootstrap" % "3.3.4"
libraryDependencies += "com.h2database" % "h2" % "1.4.192"
libraryDependencies += evolutions
      