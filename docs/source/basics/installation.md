# Installation

## Requirements

-	Java Runtime 17 or newer

## Precompiled JAR

<!--start:download-release-->
{download}`Latest RELEASE version (0.1.0-cloudevents) (not published on maven repositories)`<!--end:download-release-->

<!--start:download-snapshot-->
<!--end:download-snapshot-->

## Maven Dependency

```xml
<dependency>
	<groupId>de.fraunhofer.iosb.ilt.faaast.service</groupId>
	<artifactId>starter</artifactId>
	<version>0.1.0-cloudevents</version>
</dependency>
```

## Gradle Dependency

```groovy
implementation 'de.fraunhofer.iosb.ilt.faaast.service:starter:0.1.0-cloudevents'
```

## Build from Source

```sh
git clone https://github.com/FraunhoferIOSB/FAAAST-Service
cd FAAAST-Service
mvn clean install
```