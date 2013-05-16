import com.typesafe.sbt.SbtStartScript

organization  := "ecol"

version       := "0.1"

scalaVersion  := "2.10.0"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/",
  "Twitter's Repository" at "http://maven.twttr.com/"
)

libraryDependencies ++= Seq(
  "io.spray"            %   "spray-can"     % "1.1-M7",
  "io.spray"            %   "spray-routing" % "1.1-M7",
  "io.spray"            %   "spray-testkit" % "1.1-M7",
  "io.spray"			%%	"spray-json"	% "1.2.3",
  "com.typesafe.akka"   %%  "akka-actor"    % "2.1.0",
  /*
  "com.twitter" 		% 	"cassie"	 	% "0.19.0" excludeAll( 
  	ExclusionRule(organization = "com.sun.jdmk"), 
  	ExclusionRule(organization = "com.sun.jmx"), 
  	ExclusionRule(organization = "javax.jms")
	),
  */
  "com.netflix.astyanax"   	% "astyanax-core"    	% "1.56.34",
  "com.netflix.astyanax"   	% "astyanax-thrift"    	% "1.56.34",
  "com.netflix.astyanax"   	% "astyanax-cassandra"  % "1.56.34",
  "org.specs2"          %%  "specs2"        % "1.14" % "test"
)

seq(Revolver.settings: _*)

seq(SbtStartScript.startScriptForClassesSettings: _*)