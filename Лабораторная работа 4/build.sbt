ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "zookeeper-animal-client"
  )

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.8.1" % "compile"
