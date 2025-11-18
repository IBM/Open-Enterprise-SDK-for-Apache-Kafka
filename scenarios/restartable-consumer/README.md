# Restartable Consumer COBOL Sample using Open-Enterprise-SDK-for-Apache-Kafka

This repository contains Kafka consumer samples upgraded using the base samples provided in the main repo. This sample demonstrates batch restartable functionality along with the SDK — a critical feature for long-running batch jobs that need to recover gracefully from failures. These samples illustrate techniques for checkpointing, recovery, and job control using COBOL in a mainframe environment.

---

## Table of Contents

1. [IXYCONSI.cpy](copy/IXYCONSI.cpy) - Use this updated input copybook for consumer base program IXYCONS.
2. [IXYCONSO.cpy](copy/IXYCONSO.cpy) - Use this updated output copybook for consumer base program IXYCONS.
3. [IXYSCONS.cbl](src/IXYSCONS.cbl) - Consumer sample updated to handle Basic restartable functionality using low level API's.
4. [IXYJCONS.jcl](../../jcl/IXYJCONS.jcl) - Use this JCL to compile the updated base consumer module IXYSCONS.
5. [IXYCON64.cbl](src/IXYCON64.cbl) - Consumer Application program which invokes the updated base consumer module IXYSCONS by providing the details of partition and offset.
6. [IXYJCN64.cbl](../../jcl/IXYJCN64.jcl) - Use this JCL to compile the updated application consumer program IXYCON64.cbl
7. [CHKPTFIL.cpy](copy/CHKPTFIL.cpy) - Use this copybook to create a Checkpoint file of size 2049 similar to other config files. Provide the partition and offset. Offset can be set to zero.
8. [IXYCCONF.CONFIG](conf/IXYCCONF.CONFIG) - use this consumer configuration file which includes enabling end of partition to true for detecting the end of partition during consume. Update other details accordingly.
9. [IXYJRC64.jcl](jcl/IXYJRC64.jcl) - Use this JCL to run the consumer application program by providing the checkpoint file created. PARM value should be RESTART incase of job restart or it can be NONE.

## Prerequisites

1. Open-Enterprise-SDK-for-Apache-Kafka installed.
2. Kafka broker should have messages produced to the topic.

## Features
### First Processing :
1. Reads the checkpoint file for partition numbers. Offsets are set to -2 which points to the beginning of partition.
2. Low level API's are invoked for initialising and consuming from each partition.
3. Destroy after end of partitions is reached for all partitions.

### Restart Processing:

1. Reads the checkpoint data for each partition and offset provided.
2. Start consuming from the next offset using low level API's Consume Start and Consume. This Skips already processed records. Continues from the last known good state.
3. Consume stop when end of partition is reached for each of these partitions.

## How to Run

1. Compile the COBOL program IXYSCONS using the JCL IXYJCONS.
2. Simulate a failure by forcing an abend or interrupt in application program IXYCON64 after consuming certain records. Use WS-CONSUME-CNT which has the count of messages consumed. Either use PERFORM UNTIL WS-CONSUME-CNT is equal to some value or add explicit abend after consuming certain messages.
3. Compile the COBOL program IXYCON64 using the JCL IXYJCN64. 
4. Have the Checkpoint file created using the copybook CHKPTFIL.cpy with size 2049 having all partitions and offset can be set to -2 which denotes the beginning of partition. Please Note Partition is needed since Metadata API is not available currently and it is work in progress.
5. Execute the job using the corresponding run JCL IXYJRC64, so that job fails after consuming certain messages. Note that COMMIT happens to the checkpoint file after every record. 
6. Restart the same job again which reads the checkpoint file and resumes processing from the next offset until end of partition is reached for each partition.

---
