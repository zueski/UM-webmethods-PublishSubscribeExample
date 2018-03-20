# UM-webmethods-PublishSubscribeExample
Example on how to publish a message to Universal Messaging using the protocol buffer definition from webMethods Integration Server

## Build requirements
 - Add nClient.jar from your UM instance to your local maven repo, adjusting versions as needed:
 `mvn install:install-file -Dfile=nClient.jar -DgroupId=com.softwareag -DartifactId=nClient -Dversion=9.12.0 -Dpackaging=jar -DgeneratePom=true`
 - create and deploy java service on Integration Service to extract protobuf definitions, referring to getProtoDef.frag
 
## Build process
Example is already setup with the input, so steps 1-3 are done alread
 1. Run the wM IS service, getProtoDef, with the input document and save the output to src/main/resources/protobuf as %message%.proto (e.g., AmGlDocs_Event_EventUDM.proto)
 2. Generate message java classes by running `mvn generate-sources`
 3. Update message generation to use new java classes (more below, see Protocol Buffers)
 4. Compile jar file by running `mvn package`
 
 ## Running the example
Run the script, substituting the UM realm (RNAME) and document channel (CHANNAME) as needed:
 `runPublisher.bat CHANNAME:wm/is/AmGlDocs/Event/EventUDM RNAME:nhp://uslxd10600a:9000 COUNT:1`
Note:  The name for the channel should always match the value for the document property called Provider definition (e.g."wm/is/AmGlDocs/Event/EventUDM")
 
## Protocol Buffers
Formatting the message is done with [Google's Protocol Buffers](https://developers.google.com/protocol-buffers/).  For a deeper understanding on how the generated code is created, read [this](https://developers.google.com/protocol-buffers/docs/reference/java-generated)

## Basics:
The generated the message code works by providing a builder interface pattern to the message. First, you need to create base message builder:
```java
com.amway.canonical.EventUDM.AmGlDocs_Event_EventUDM.Builder builder = com.amway.canonical.EventUDM.AmGlDocs_Event_EventUDM.newBuilder();
```
Now we need to got through and set the fields, in this case, there are only a couple that are needed:
```java
// set main payload
builder.setPayload(myPayloadField);
// create nested document
builder.getEventInfoBuilder()
	.setEventID(myEventID)
	.setEventCode(myEventCode)
	.setSourceTimestamp(mySourceTimestamp)
	.setSourceHost(InetAddress.getLocalHost().getHostName());
```
One last section to make the message readable for Integration Server, all published messages should have an _env section:
```java
String uuid = UUID.randomUUID().toString();
builder.getEnvBuilder()
	.setLocale("") // no need to set, can leave empty
	.setActivation(uuid) // this should be unique unless repesonding to a publish and wait
	.setBusinessContext(myEventID)
	.setUuid(uuid)
	.setPubId(uuid);
```
Now we just need to create the event that can be published and call publish
```java
nConsumeEvent evt1 = new nProtobufEvent(builder.build().toByteArray(), "AmGlDocs_Event_EventUDM");
myChannel.publish(evt1);
```
Note:  The name for the message type should always match the name of the message in the .proto file