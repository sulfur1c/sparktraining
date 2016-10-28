name := "sparktraining"
 
version := "1.0" 
 
scalaVersion := "2.10.5" 
 
//add this code 
libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "1.2.1"
)