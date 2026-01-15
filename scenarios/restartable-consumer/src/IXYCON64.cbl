       CBL PGMNAME(LONGMIXED) NODLL NOEXPORTALL
      ******************************************************************
      * Copyright IBM Corp. 2025
      *
      * Licensed under the Apache License, Version 2.0 (the "License");
      * you may not use this file except in compliance with the License.
      * You may obtain a copy of the License at
      *
      *     http://www.apache.org/licenses/LICENSE-2.0
      *
      * Unless required by applicable law or agreed to in writing
      * , software distributed under the License is distributed on an
      * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
      * either express or implied. See the License for the specific
      * language governing permissions and limitations under the
      * License.
      ******************************************************************
      * MAIN PROGRAM IXYCON64
     ******************************************************************
      * This COBOL program is designed to consume messages from a Kafka
      * topic using a partition-based approach. It initializes a Kafka
      * consumer, reads configuration and checkpoint data, processes
      * messages from each partition, and updates the checkpoint file
      * after consumption. This program uses CONFFILE to get the
      * Configuration parameters needed. It uses TOPICFIL to get the
      * topic details. It uses CHKPTFIL to get the partition and offset
      * details.
      *
      * Please Note that it is just a sample application program not a
      * production ready module.
      *
      * This COBOL program is designed to:
      *
      * 1. Initialize a Kafka consumer.
      * 2. Read configuration, topic and checkpoint data.
      * 3. Consume messages from multiple Kafka partitions.
      * 4. Display consumed message.
      * 5. Update checkpoint data after consumption.
      * 6. Clean up resources before exiting.
      *
      * The program should be modified with the following changes:
      * 1) The value of PART-VAL should be set to the target partition
      *    value.
      * 2) The value of PART-LIST-SIZE should be set to the size of
      *    topic partition list.
      * 3) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 4) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events
      * 5) TOPICFIL - This is the file which contains the topic details.
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the topic length crosses 2049 bytes.
      * 6) CHKPTFIL - This is the file which contains the partition and
      *    offset details. Provide the individual partition numbers as
      *    each record in the file. For First processing, offset is not
      *    needed and can be 0. Provide the offset for each partitions
      *    to be able to restart from the next available offset. If
      *    restarting after first processing, then offset would be
      *    taken automatically after the first run.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYCON64'.
       ENVIRONMENT DIVISION.
        INPUT-OUTPUT SECTION.
         FILE-CONTROL.
           SELECT CONFFILE ASSIGN TO CONFFILE
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT TOPICFIL ASSIGN TO TOPICFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT CHKPTFIL ASSIGN TO CHKPTFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.
       DATA DIVISION.
        FILE SECTION.
         FD CONFFILE
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  KAFKA-CONFIG-FILE.

       01 KAFKA-CONFIG-FILE.
          05 KAFKA-CONFIG-REC         PIC X(2049).

         FD TOPICFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  TOPIC-DATA.

       01 TOPIC-DATA.
          05 TOPIC-DATA-REC           PIC X(2049).

         FD CHKPTFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  CHECK-POINT-FILE.

       01 CHECK-POINT-FILE.
          05 CHKPT-PARTITION          PIC S9(9) BINARY.
          05 CHKPT-OFFSET             PIC S9(18) BINARY.

        WORKING-STORAGE SECTION.
      ******************************************************************
      * WORKING-STORAGE SECTION - Kafka Consumer Program               *
      ******************************************************************
      * Consumer Values
       01 PART-VAL                    PIC S9(9) BINARY
                                                  VALUE -1.
       01 PART-LIST-SIZE              PIC S9(9) BINARY
                                                  VALUE 1.
       01 MSGFLGS-VAL                 PIC X(01)   VALUE X'02'.
       01 TIMEOUT-MS                  PIC S9(9) BINARY
                                                  VALUE 8000.
       01 WS-CONSUME-CNT              PIC 9(9)    VALUE 0.
       01 WS-END-CONSUMER             PIC X(1)    VALUE 'N'.
       01 TOPIC-LENGTH                PIC S9(4) BINARY
                                                  VALUE 0.
       01 WS-DISPLAY-ERR              PIC S9(9) SIGN IS LEADING
                                                  SEPARATE.
       01 KAFKA-MSG-TEMP              PIC X(1024).
       01 WS-RCNT                     PIC 9(9)    VALUE 0.
       01 MC-REMAINDER                PIC 9(4)    VALUE 0.
       01 MC-QUOTIENT                 PIC 9(4)    VALUE 0.

      * Checkpoint Record
       01 WS-CHECK-POINT-REC.
          05 WS-CHECK-POINT-FILE OCCURS 15 TIMES.
             10 WS-RESTART-PARTITION  PIC S9(9) BINARY.
             10 WS-RESTART-OFFSET     PIC S9(18) BINARY.

      * Counters
       01 WS-CNT1                     PIC 9(9)    VALUE 1.
       01 WS-PCNT                     PIC 9(9)    VALUE 1.

      * File Status
       01 WS-FILE-STATUS              PIC 9(2).
       01 WS-EOF-SW                   PIC X(1).
          88 WS-EOF                               VALUE 'Y'.
          88 WS-NOT-EOF                           VALUE 'N'.

      * Configuration File Parsing
       01 WS-CNT                      PIC S9(9) BINARY
                                                  VALUE 0.
       01 WS-PARMLEN                  PIC S9(9) BINARY
                                                  VALUE 0.
       01 WS-VALLEN                   PIC S9(9) BINARY
                                                  VALUE 0.
       01 WS-DELIMITER-POS            PIC S9(9) BINARY
                                                  VALUE 0.

       01 KAFKA-CONFIG-DATA.
          05 KAFKA-CONFIG-PARM        PIC X(1024).
          05 WS-DELIMITER             PIC X       VALUE '='.
          05 KAFKA-CONFIG-VALUE       PIC X(1024).

      * Input/Output for Consumer Program
      * >>DATA 31 needs to be provided if the calling module is
      * compiled in 64 bit and is calling IXYSCONS. This is needed
      * in order to ensure that pointers contain the right data.
       >>DATA 31
       01 CONSUMER-INPUT.
            COPY IXYCONSI.
       01 CONSUMER-OUTPUT.
            COPY IXYCONSO.
       01 CONSUMER-PGM                PIC X(8)    VALUE "IXYSCONS".

      * Linkage Section
       LINKAGE SECTION.
       01 KAFKA-MSG-ASCII             PIC X(1024).

       PROCEDURE DIVISION.

           DISPLAY "KAFKA AMODE 64 PROGRAM"
           PERFORM READ-CONSUMER-TOPIC
           PERFORM READ-CONSUMER-CONFIG
           PERFORM READ-CHKPT-FILE
           PERFORM INIT-KAFKA-CONSUMER

           PERFORM UNTIL(WS-PCNT > WS-RCNT)
                   MOVE 'N' TO WS-END-CONSUMER
                   MOVE RESTART-PARTITION(WS-PCNT) TO CONSUME-PARTITION

                   PERFORM UNTIL WS-END-CONSUMER = 'Y'
                           PERFORM KAFKA-CONSUME-MESSAGE
                           PERFORM WRITE-CHKPT-FILE
                   END-PERFORM

                   ADD 1 TO WS-PCNT
           END-PERFORM

           DISPLAY "KAFKA MESSAGE CONSUME DONE"
           DISPLAY "NUMBER OF KAFKA MESSAGES CONSUMED : " WS-CONSUME-CNT
           PERFORM DESTROY-KAFKA-CONSUME
           GOBACK
           .

       READ-CHKPT-FILE.
      ******************************************************************
      * READ-CHKPT-FILE para is responsible for reading a checkpoint
      * file that stores Kafka partition and offsets, which are used to
      * resume message consumption from the particular point. It opens
      * the file in input mode and reads each record until the end of
      * the file is reached. Increments the offset by 1 to avoid 
      * reprocessing the last consumed message if offsets are 0 or 
      * greater than 0. After all records are read, the total number of 
      * partitions is stored in TOTAL-PARTNOS, and the file is closed.
      ******************************************************************
           OPEN INPUT CHKPTFIL
           SET WS-NOT-EOF TO TRUE
           PERFORM UNTIL WS-EOF
                   READ CHKPTFIL
                   AT END
                      SET WS-EOF TO TRUE
                   NOT AT END
                       ADD 1 TO WS-RCNT

                       MOVE CHKPT-PARTITION TO
                          WS-RESTART-PARTITION(WS-RCNT)
                          RESTART-PARTITION(WS-RCNT)
                       MOVE CHKPT-OFFSET TO WS-RESTART-OFFSET(WS-RCNT)

                       IF WS-RESTART-OFFSET(WS-RCNT) >= 0
                          MOVE WS-RESTART-OFFSET(WS-RCNT) TO
                             RESTART-OFFSET(WS-RCNT)
                          ADD 1 TO RESTART-OFFSET(WS-RCNT)

                       END-IF
                   END-READ
           END-PERFORM
           MOVE WS-RCNT TO TOTAL-PARTNOS
           CLOSE CHKPTFIL
           .

       READ-CONSUMER-CONFIG.
      ******************************************************************
      * The READ-CONSUMER-CONFIG para reads Kafka consumer
      * configuration parameters from an input file. It opens the
      * configuration file and processes each line until the end of the
      * file is reached. Lines beginning with a # are treated as
      * comments and skipped. For valid configuration lines, the program
      * identifies the delimiter (typically =), separates the parameter
      * name and its value, trims any leading or trailing spaces, and
      * stores them in indexed arrays (CONFIG-NAME and CONFIG-VALUE).
      * It also appends a LOW-VALUE character to mark the end of each
      * string, which is useful for string termination in COBOL. The
      * program keeps track of the number of parameters read and closes
      * the file after processing.
      ******************************************************************

           OPEN INPUT CONFFILE
           SET WS-NOT-EOF TO TRUE
           PERFORM UNTIL WS-EOF
                   READ CONFFILE
                   AT END
                      SET WS-EOF TO TRUE
                   NOT AT END
                       IF KAFKA-CONFIG-REC(1:1) NOT = '#'
                          MOVE 0 TO WS-DELIMITER-POS

                          INSPECT KAFKA-CONFIG-REC TALLYING
                             WS-DELIMITER-POS
                             FOR CHARACTERS BEFORE WS-DELIMITER

                          IF WS-DELIMITER-POS NOT = 0
                             MOVE KAFKA-CONFIG-REC(1:WS-DELIMITER-POS)
                                TO
                                KAFKA-CONFIG-PARM
                             MOVE KAFKA-CONFIG-REC(WS-DELIMITER-POS + 2:
                                ) TO
                                KAFKA-CONFIG-VALUE
                          END-IF

                          ADD 1 TO NUM-OF-PARMS
                          ADD 1 TO WS-CNT

                          COMPUTE WS-PARMLEN = FUNCTION LENGTH
                             (
                             FUNCTION TRIM(KAFKA-CONFIG-PARM))
                          COMPUTE WS-VALLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                          MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                             CONFIG-NAME(WS-CNT)(1:WS-PARMLEN)
                          MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                             CONFIG-VALUE(WS-CNT)(1:WS-VALLEN)
                          MOVE LOW-VALUE TO CONFIG-NAME(WS-CNT)
                             (WS-PARMLEN + 1:)
                          MOVE LOW-VALUE TO CONFIG-VALUE(WS-CNT)
                             (WS-VALLEN + 1:)
                       END-IF
                   END-READ
           END-PERFORM

           CLOSE CONFFILE
           .

       READ-CONSUMER-TOPIC.
      ******************************************************************
      * The READ-CONSUMER-TOPIC paragraph reads the Kafka topic name
      * from an input file. It opens the file, reads a single record
      * containing the topic name, and calculates the length of the
      * topic string by tallying characters before the first space. The
      * topic name is then trimmed of any leading or trailing spaces and
      * stored in the KAFKA-TOPIC-NAME variable. A LOW-VALUE character
      * is appended to mark the end of the string, ensuring proper
      * termination. Finally, the file is closed. This routine ensures
      * that the Kafka consumer is correctly initialized with the
      * intended topic for message consumption.
      ******************************************************************
           OPEN INPUT TOPICFIL

           READ TOPICFIL

           INSPECT TOPIC-DATA-REC TALLYING TOPIC-LENGTH
              FOR CHARACTERS BEFORE ' '

           MOVE FUNCTION TRIM(TOPIC-DATA-REC) TO
              KAFKA-TOPIC-NAME(1:TOPIC-LENGTH)
           MOVE LOW-VALUE TO KAFKA-TOPIC-NAME(TOPIC-LENGTH + 1:)

           CLOSE TOPICFIL
           .

       INIT-KAFKA-CONSUMER.
      ******************************************************************
      * The INIT-KAFKA-CONSUMER paragraph initializes the Kafka consumer
      * by setting up required parameters and invoking the consumer
      * program. It sets the Kafka type to consumer mode
      * (KAFKA-TYPE-PC = 1) and assigns values for partition list size,
      * partition value, message flags, and timeout duration. The action
      * code 'I' is used to indicate initialization. tTe Kafka consumer
      * program (IXYSCONS) is called using the prepared input structure.
      * Upon return, the program checks the response code. If an error
      * is detected (KAFKA-MSG-RESPONSE NOT 0), it displays the error
      * message and code, sets a return code of 16, and exits.
      * Otherwise, it confirms successful initialization by displaying
      * the response message.
      ******************************************************************
      * KAFKA-TYPE-PC is 0 for PRODUCER
      * KAFKA-TYPE-PC is 1 for CONSUMER
           MOVE 1 TO KAFKA-TYPE-PC
           MOVE PART-LIST-SIZE TO KAFKA-PART-LIST-SIZE
           MOVE PART-VAL TO PARTITION-VALUE
           MOVE MSGFLGS-VAL TO MSGFLAGS-VALUE
           MOVE TIMEOUT-MS TO TIMEOUT-MS-VALUE

           MOVE 'I' TO KAFKA-ACTION
           DISPLAY "KAFKA CONSUMER INIT BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
              RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
              DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
              MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                 WS-DISPLAY-ERR
              DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
              MOVE 16 TO RETURN-CODE
              GOBACK
           ELSE
              DISPLAY FUNCTION TRIM(KAFKA-MSG)
           END-IF
           .

       KAFKA-CONSUME-MESSAGE.
      ******************************************************************
      * The KAFKA-CONSUME-MESSAGE paragraph is responsible for
      * retrieving events from Kafka by invoking the consumer program
      * IXYSCONS with the action code 'C', which signifies a consume
      * operation. A special case is handled for error code -191, which
      * indicates that the end of the partition has been reached. This
      * is not treated as a critical error. For other error codes, sets
      * a return code of 16, and prepares to exit. If the message is
      * successfully consumed, the payload is converted from EBCDIC to
      * ASCII and displayed along with its length. The program then
      * updates the restart offset for the corresponding partition to
      * ensure accurate checkpointing, allowing the consumer to resume
      * from the correct position in future runs.
      ******************************************************************
           MOVE 'C' TO KAFKA-ACTION
           DISPLAY "KAFKA CONSUME BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
              RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
              MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                 WS-DISPLAY-ERR
              MOVE 'Y' TO WS-END-CONSUMER
              IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = -191
                 DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
                 DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
                 MOVE 16 TO RETURN-CODE
              END-IF

           ELSE
              SET ADDRESS OF KAFKA-MSG-ASCII
                 TO KAFKA-PAYLOAD-64

              MOVE FUNCTION DISPLAY-OF(
                 FUNCTION
                 NATIONAL-OF(KAFKA-MSG-ASCII 819) 1047)
                 TO KAFKA-MSG-TEMP

              DISPLAY "MESSAGE CONSUMED : "
                      KAFKA-MSG-TEMP(1:KAFKA-PAYLOAD-LEN)
              DISPLAY "MESSAGE LENGTH : " KAFKA-PAYLOAD-LEN
              ADD 1 TO WS-CONSUME-CNT

              IF WS-RCNT > 0
                 MOVE 1 TO WS-CNT1

                 PERFORM UNTIL WS-CNT1 > WS-RCNT
                         IF WS-RESTART-PARTITION(WS-CNT1) =
                            PAYLOAD-PARTITION
                            MOVE PAYLOAD-OFFSET TO WS-RESTART-OFFSET
                               (WS-CNT1)
                         END-IF
                         ADD 1 TO WS-CNT1
                 END-PERFORM
              END-IF
           END-IF
           .

       WRITE-CHKPT-FILE.
      ******************************************************************
      * The WRITE-CHKPT-FILE paragraph updates the checkpoint file with
      * the latest Kafka partition offsets after message consumption.
      * It opens the checkpoint file in I-O mode and reads each record
      * until the end of the file is reached. For each record, it
      * iterates through the list of consumed partitions and checks if
      * the partition in the file matches any of the consumed
      * partitions. If a match is found, it updates the corresponding
      * offset with the latest value. The updated record is then
      * rewritten to the file using the REWRITE statement. This ensures
      * that the checkpoint file reflects the most recent offsets,
      * allowing the program to resume accurately from the last consumed
      * message in future runs. Once all records are processed, the file
      * is closed.
      ******************************************************************
           OPEN I-O CHKPTFIL
           SET WS-NOT-EOF TO TRUE
           PERFORM UNTIL WS-EOF
                   READ CHKPTFIL
                   AT END
                      SET WS-EOF TO TRUE
                   NOT AT END
                       MOVE 1 TO WS-CNT1
                       PERFORM UNTIL WS-CNT1 > WS-RCNT
                               IF WS-RESTART-PARTITION(WS-CNT1) =
                                  CHKPT-PARTITION
                                  MOVE WS-RESTART-OFFSET(WS-CNT1) TO
                                     CHKPT-OFFSET
                               END-IF

                               REWRITE CHECK-POINT-FILE
                               ADD 1 TO WS-CNT1

                       END-PERFORM
                   END-READ
           END-PERFORM
           CLOSE CHKPTFIL
           .

       DESTROY-KAFKA-CONSUME.
      ******************************************************************
      * The DESTROY-KAFKA-CONSUME paragraph is responsible for cleaning
      * up Kafka consumer resources after all messages have been
      * consumed. It sets the Kafka action to 'D' to indicate a destroy
      * operation. The program then calls the Kafka consumer module
      * IXYSCONS using the input structure. If the response indicates an
      * error, it displays the error message and code, sets the return
      * code to 16, and prepares to exit.
      ******************************************************************
           MOVE 'D' TO KAFKA-ACTION
           DISPLAY "KAFKA CONSUMER DESTROY BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
              RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
              DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
              MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                 WS-DISPLAY-ERR
              DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
              MOVE 16 TO RETURN-CODE
           ELSE
              DISPLAY FUNCTION TRIM(KAFKA-MSG)
           END-IF
           .

       END PROGRAM 'IXYCON64'.