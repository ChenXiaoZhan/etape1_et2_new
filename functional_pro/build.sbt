// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.8"


name := "Jeremy Trullier, Francis Tran, Zhan,Chen Sebastien Chen"
organization := "efrei"
version := "1.0"


libraryDependencies += "org.typelevel" %% "cats-core" % "1.6.0"
libraryDependencies += "org.apache.kafka"%"kafka-clients"%"2.1.0"
//libraryDependencies += "com.typesafe.play"%%"play-json"% "2.7.2"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
//libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21"


// https://mvnrepository.com/artifact/javax.mail/javax.mail-api
libraryDependencies += "javax.mail" % "javax.mail-api" % "1.6.2"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"

