# Restartable Consumer COBOL Sample using Open-Enterprise-SDK-for-Apache-Kafka

This repository contains Kafka consumer samples upgraded using the base samples provided in the main repo. This sample demonstrates batch restartability functionality along with the SDK — a critical feature for long-running batch jobs that need to recover gracefully from failures. These samples illustrate techniques for checkpointing, recovery, and job control using COBOL in a mainframe environment.

---

## Table of Contents

1. [Samples/Restartable Consumer/Copy/IXYCONSI.cpy] - Use this updated input copybook for consumer base program IXYCONS.
2. [Samples/Restartable Consumer/Copy/IXYCONSO.cpy] - Use this updated output copybook for consumer base program IXYCONS.
3. [Samples/Restartable Consumer/Src/IXYSCONS.cbl] - Consumer sample updated to handle Basic restartable batch job using a checkpoint file.
4. [jcl/IXYJCONS.jcl] - Use this JCL to compile the updated base consumer module
5. [Samples/Restartable Consumer/Src/IXYCON64.cbl] - Consumer Application program which invokes the updated base consumer module IXYSCONS    by providing the details of partition and offset, in case of restart.
6. [jcl/IXYJCN64.cbl] - Use this JCL to compile the updated application consumer program IXYCON64.cbl
7. [Samples/Restartable Consumer/Copy/CHKPTFIL.cpy] - Use this copybook to create a Checkpoint file of size 2049 similar to other config files. Provide the partition and offset. Offset can be set to zero.
8. [Samples/Restartable Consumer/Conf/IXYCCONF.CONFIG] - use this consumer configuration file which includes enabling end of partition to true for detecting the end of partition when it occurs. Update other details accordingly.
9. [Samples/Restartable Consumer/Jcl/IXYJRC64.jcl] - Use this JCL to run the consumer application program by providing the checkpoint file created. PARM value should be RESTART incase of job restart or it can be NONE.

## Prerequisites

1. Open-Enterprise-SDK-for-Apache-Kafka installed.
2. Kafka broker should have messages produced to the topic.

## Features
### First processing :
1. Reads the checkpoint file for partitions count and partition numbers. If not available, then checkpoint file is updated with the partition and offset.
2. High level API's are invoked for consuming by Subscribing to the topic and polling continuously.
3. Destroy after end of partitions is reached for all partitions.

### restart processing:

1. Reads the checkpoint data for each partition and offset.
2. Start consuming from the next offset using low level API's Consume Start and Consume. This Skips already processed records. Continues from the last known good state.
3. Consume stop when end of partition is reached for each of these partitions.

## How to Run

1. Compile the COBOL program IXYSCONS using the JCL IXYJCONS.
2. Simulate a failure by forcing an abend or interrupt in application program IXYCON64 after consuming certain records. Use WS-CONSUME-CNT which has the count of messages consumed. Either use PERFORM UNTIL WS-CONSUME-CNT is some value or add explicit abend after consuming certain messages.
3. Compile the COBOL program IXYCON64 using the JCL IXYJCN64. 
4. Have the Checkpoint file created using the copybook CHKPTFIL.cpy with size 2049 having all partitions and offset can be set to 0.
5. Execute the job using the corresponding run JCL IXYJRC64 with PARM as NONE and new checkpoint file, so that job fails after consuming certain messages. Note that COMMIT happens to the checkpoint file after every 50 records and after processing designated number of messages or after consuming all messages from partitions. 
6. Restart the same job again with using the PARM value as 'RESTART' which reads the checkpoint file and resumes processing until end of partition is reached.

---
