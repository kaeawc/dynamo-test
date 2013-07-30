import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

	val appName = "dynamo-test"
	val appVersion = "0.1"
	val appDependencies = Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.5.2",
		"com.typesafe.play.extras" %% "iteratees-extras" % "1.0.1"
	)
	
	val main = play.Project(
		appName,
		appVersion,
		appDependencies
	).settings(
		scalacOptions ++= Seq(
			"-encoding",
			"UTF-8",
			"-deprecation",
			"-unchecked",
			"-feature"
		)
	)
}
