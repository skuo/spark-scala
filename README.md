# spark-scala

## Install Scala IDE

Download the Scala IDE from http://scala-ide.org/, which is custom build of Eclipse specifically for Scala.  Since it is not signed,
go to "System Preferences > Security" to allow the downloaded app to run.

## Setup SBT Elicpse Plugin

It is easiest to use sbt to configure a Scala project in Eclipse.  The following sets up sbt eclipse plugin

```bash
# Create the directory if necessary
$ cat > ~/.sbt/1.0/plugins/plugins.sbt
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
^-D
```

Now use sbt to create a scala project

```bash
sbt eclipse
```

## Start Up Scala IDE and import the newly created project

Start up Scala IDE and click "File > Open Projects from File System ..."

## README-Spark.md

Please reference README-Spark.md for building and submitting Scala job to Spark.