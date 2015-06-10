val datastax = "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.6"

lazy val commonSettings = Seq(
	scalaVersion := "2.11.6"
)

lazy val root = (project in file(".")).
	settings(commonSettings: _*).
	settings(
		name := "connector",
		libraryDependencies += datastax,
		mainClass := Some("MyApp")
	)


