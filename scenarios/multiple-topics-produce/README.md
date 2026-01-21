# Producing Kafka Messages to multiple Topics COBOL Sample using Open-Enterprise-SDK-for-Apache-Kafka
This folder contains Kafka producer samples that demonstrate producing messages to multiple Kafka topics from a single producer instance. This sample showcases advanced producer functionality where a single COBOL application can efficiently produce messages up to 5 different topics using the SDK — a critical feature for applications that need to distribute data across multiple topics in a mainframe environment.

## Table of Contents
[IXYPRDSI.cpy](copy/IXYPRDSI.cpy) - Updated input copybook for producer base program that includes support for multiple topics.
[IXYSPRDS.cbl](src/IXYSPRDS.cbl) - Producer sample updated to handle multiple topic instances, maintaining separate topic references (KAFKA-TOPIC-REF1 through KAFKA-TOPIC-REF5) for efficient message routing.
[IXYJPRDS.jcl](/jcl/IXYJPRDS.jcl)- Use this JCL to compile the updated base producer module IXYSPRDS in AMODE 64.
[IXYPRD64.cbl](src/IXYPRD64.cbl) - Producer application program that reads multiple topics from TOPICFIL and invokes the updated base producer module IXYSPRDS to produce messages to different topics.
[IXYJPR64.jcl](/jcl/IXYJPR64.jcl) - Use this JCL to compile the application producer program IXYPRD64 in AMODE 64.

## Prerequisites
Open-Enterprise-SDK-for-Apache-Kafka installed.
Kafka broker accessible with appropriate topics created.
Configuration file (IXYPCONF) with producer settings.
Topic file (IXYTCONF) containing the list of topic names (one per line, up to 5 topics).
Event file (EVENTFIL) containing messages to be produced.

## Features
### Multiple Topic Support:
Reads up to 5 topic names from TOPICFIL during initialization.
Creates separate topic instances for each topic, maintaining individual topic references (KAFKA-TOPIC-REF1 through KAFKA-TOPIC-REF5).
Routes messages to specific topics based on the KAFKA-TOPIC-NBR field in the input copybook.
Efficiently manages multiple topic handles within a single producer instance.
### Producer Functionality:
Initializes Kafka producer with configuration parameters from CONFFILE.
Creates topic instances for all topics specified in TOPICFIL.
Produces messages from EVENTFIL to designated topics.
Supports message delivery callbacks for tracking message status.
Handles proper cleanup and destruction of all topic instances.

## How to Run
Prepare the IXYTCONF file with topic names (one per line, maximum 5 topics):

TOPIC1
TOPIC2
TOPIC3

Prepare the IXYPCONF file with Kafka producer configuration parameters (e.g., bootstrap.servers, security settings).

Prepare the EVENTFIL with messages to be produced (1024 bytes per record).

Compile the base producer module IXYSPRDS using JCL IXYJPRDS:

Update @@JOBCARD@@, @@IXYHLQ@@, @@CEEHLQ@@, @@IGYHLQ@@ as needed.
Compile the application program IXYPRD64 using JCL IXYJPR64:

Update the same parameters as above.

Adjust PART-VAL for partition targeting (-1 for automatic).
Set MSGFLGS-VAL for message flags.
Configure TIMEOUT-MS for blocking timeout.
Execute the producer application using the appropriate run JCL, providing:

CONFFILE DD pointing to configuration file
TOPICFIL DD pointing to topics file
EVENTFIL DD pointing to messages file
Monitor the output for successful message production to multiple topics.

## Key Implementation Details
Topic Management: The IXYSPRDS.cbl module maintains separate pointers for up to 5 topics (KAFKA-TOPIC-REF1 through KAFKA-TOPIC-REF5).
Topic Selection: Messages are routed to topics based on the KAFKA-TOPIC-NBR field (88-level conditions: KAFKA-TOPIC-1 through KAFKA-TOPIC-5).
Initialization: All topics are initialized during the KAFKA-INIT phase before message production begins.
Resource Management: Proper cleanup ensures all topic instances are destroyed when the producer terminates.