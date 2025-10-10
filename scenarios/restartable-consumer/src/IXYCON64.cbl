
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
      * This is a sample program compiled in 64 bit addressing mode.
      * This program calls IXYSCONS module and consume the message
      * from Kafka. This program uses CONFFILE to get the
      * Configuration parameters needed. It uses TOPICFIL to get the
      * topic details. It uses CHKPTFIL to get the partition and offset
      * details.
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
      *    if the topic length crosses 1024 bytes.
      * 6) CHKPTFIL - This is the file which contains the partition and
      *    offset details. This is mostly used during the restart 
      *    scenario.
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
            05 KAFKA-CONFIG-REC      PIC X(2049).

         FD TOPICFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  TOPIC-DATA.

         01 TOPIC-DATA.
            05 TOPIC-DATA-REC     PIC X(2049).

         FD CHKPTFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  CHECK-POINT-FILE.

         01 CHECK-POINT-FILE.
            05 CHKPT-PARTITION    PIC S9(9) BINARY.
            05 CHKPT-OFFSET       PIC S9(18) BINARY.
        WORKING-STORAGE SECTION.
      ******************************************************************
      *  CONSUMER Values
      ******************************************************************
         01 PART-VAL            PIC S9(9)  BINARY VALUE -1.
         01 PART-LIST-SIZE      PIC S9(09) BINARY VALUE 1.
         01 MSGFLGS-VAL         PIC X(01)  VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9)  BINARY VALUE 8000.
         01 WS-CONSUME-CNT      PIC 9(9)   VALUE 0.
         01 WS-END-CONSUMER     PIC X(1)   VALUE 'N'.
         01 TOPIC-LENGTH        PIC S9(4)  BINARY VALUE 0000.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
         01 KAFKA-MSG-TEMP      PIC X(1024).
         01 WS-RCNT             PIC 9(9) VALUE 0.
         01 MC-REMAINDER        PIC 9(04) VALUE 0.
         01 MC-QUOTIENT         PIC 9(04) VALUE 0.
         01 WS-CHECK-POINT-REC.
            05 WS-CHECK-POINT-FILE OCCURS 15 TIMES.
              10 WS-CHKPT-PARTITION    PIC S9(9) BINARY.
              10 WS-CHKPT-OFFSET       PIC S9(18) BINARY.
         01 WS-CNT1             PIC 9(9) VALUE 1.
         01 WS-PCNT             PIC 9(9) VALUE 1.
         01 WS-NEW-PARTITION-FLAG   PIC X(1).

      * File Status
         01 WS-FILE-STATUS      PIC 9(02).
         01 WS-EOF-SW           PIC X(01).
             88 WS-EOF          VALUE 'Y'.
             88 WS-NOT-EOF      VALUE 'N'.

      * Configuration file
         01 WS-CNT              PIC S9(9) BINARY VALUE 0000.
         01 WS-PARMLEN          PIC S9(9) BINARY VALUE 0000.
         01 WS-VALLEN           PIC S9(9) BINARY VALUE 0000.
         01 WS-DELIMITER-POS    PIC S9(9) BINARY VALUE 0000.

         01 KAFKA-CONFIG-DATA.
            05 KAFKA-CONFIG-PARM      PIC X(1024).
            05 WS-DELIMITER           PIC X VALUE '='.
            05 KAFKA-CONFIG-VALUE     PIC X(1024).

      * Input/Output values for Consumer program
      * >>DATA 31 needs to be provided if the calling module is
      * compiled in 64 bit and is calling IXYSCONS. This is needed
      * in order to ensure that pointers contain the right data.
       >>DATA 31
         01 CONSUMER-INPUT.
            COPY IXYCONSI.
         01 CONSUMER-OUTPUT.
            COPY IXYCONSO.
         01 CONSUMER-PGM        PIC X(8) VALUE "IXYSCONS".
       LINKAGE SECTION.
         01 KAFKA-MSG-ASCII      PIC X(1024).
         01 PARM-DATA.
           05 PARM-LENGTH            PIC S9(4) COMP.
           05 RUN-TYPE               PIC X(7).

       PROCEDURE DIVISION USING PARM-DATA.
           DISPLAY "KAFKA AMODE 64 PROGRAM"
           PERFORM READ-CONSUMER-TOPIC
           PERFORM READ-CONSUMER-CONFIG
           PERFORM READ-CHKPT-FILE

           IF RUN-TYPE = 'RESTART'
             DISPLAY "JOB RESTARTED"
             MOVE 'Y' TO RESTART-IND
           END-IF

             PERFORM UNTIL WS-CNT1 > WS-RCNT
               MOVE 'Y' TO RESTART-FLAG(WS-CNT1)
               MOVE WS-CHKPT-PARTITION(WS-CNT1) TO
               RESTART-PARTITION(WS-CNT1)
               IF RESTART-IND = 'Y'
                 MOVE WS-CHKPT-OFFSET(WS-CNT1)  TO
                   RESTART-OFFSET(WS-CNT1)
                 IF RESTART-OFFSET(WS-CNT1) > 0
                   ADD 1 TO RESTART-OFFSET(WS-CNT1)
                 END-IF
               ELSE
                 MOVE 0 TO RESTART-OFFSET(WS-CNT1)
                           WS-CHKPT-OFFSET(WS-CNT1)
               END-IF
               ADD 1 TO WS-CNT1
             END-PERFORM


           PERFORM INIT-KAFKA-CONSUMER
           PERFORM UNTIL
                         WS-END-CONSUMER = 'Y'
             PERFORM KAFKA-CONSUME-MESSAGE
           END-PERFORM
           DISPLAY "KAFKA MESSAGE CONSUME DONE"
           DISPLAY "NUMBER OF KAFKA MESSAGES CONSUMED : " WS-CONSUME-CNT
           PERFORM WRITE-CHKPT-FILE
           PERFORM DESTROY-KAFKA-CONSUME
           GOBACK
           .

       READ-CHKPT-FILE.
           OPEN INPUT CHKPTFIL
           SET WS-NOT-EOF TO TRUE
           PERFORM UNTIL WS-EOF
             READ CHKPTFIL
             AT END SET WS-EOF TO TRUE
             NOT AT END
               ADD 1 TO WS-RCNT
               MOVE CHKPT-PARTITION TO
                                 WS-CHKPT-PARTITION (WS-RCNT)
               MOVE CHKPT-OFFSET TO WS-CHKPT-OFFSET (WS-RCNT)
             END-READ
           END-PERFORM
           MOVE WS-RCNT TO RESTART-PARTNOS
           CLOSE CHKPTFIL.

       READ-CONSUMER-CONFIG.
      * CONFFILE contains the Configuration Parameters which are needed
      * for setting up the KAFKA connection. Configuration file is read
      * and parsed to extract the configuration Parameter and its
      * value. Length of Configuration Parameter and its value is
      * determined. End of string (LOW VALUES) is appended to the
      * configuration parameter and value. This file can contain
      * comments starting with '#'. Parameter and Value is delimited
      * by '='.

           OPEN INPUT CONFFILE
           SET WS-NOT-EOF TO TRUE
           PERFORM UNTIL WS-EOF
             READ CONFFILE
             AT END SET WS-EOF TO TRUE
             NOT AT END
               IF KAFKA-CONFIG-REC(1:1) NOT = '#'
                 MOVE 0 TO WS-DELIMITER-POS

                 INSPECT KAFKA-CONFIG-REC TALLYING WS-DELIMITER-POS
                   FOR CHARACTERS BEFORE WS-DELIMITER

                 IF WS-DELIMITER-POS NOT = 0
                   MOVE KAFKA-CONFIG-REC(1:WS-DELIMITER-POS) TO
                     KAFKA-CONFIG-PARM
                   MOVE KAFKA-CONFIG-REC(WS-DELIMITER-POS + 2:) TO
                     KAFKA-CONFIG-VALUE
                 END-IF

                 ADD 1 TO NUM-OF-PARMS
                 ADD 1 TO WS-CNT

                 COMPUTE WS-PARMLEN = FUNCTION LENGTH(
                   FUNCTION TRIM(KAFKA-CONFIG-PARM))
                 COMPUTE WS-VALLEN = FUNCTION LENGTH(
                   FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                 MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                      CONFIG-NAME(WS-CNT)(1:WS-PARMLEN)
                 MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                      CONFIG-VALUE(WS-CNT)(1:WS-VALLEN)
      * End of string identified using LOW VALUE in C. Hence appending
      * it to the end of each configuration and its parameters
                 MOVE LOW-VALUE TO CONFIG-NAME(WS-CNT)(WS-PARMLEN + 1:)
                 MOVE LOW-VALUE TO CONFIG-VALUE(WS-CNT)(WS-VALLEN + 1:)
               END-IF
             END-READ
           END-PERFORM

           CLOSE CONFFILE.

       READ-CONSUMER-TOPIC.
      * TOPICFIL is used to pass the Topic name to Kafka. Only
      * one topic name is being supported currently. Topic name should
      * be of maximum 1024 bytes. If its more than 1024 bytes, please
      * update the file description and use a different flat file
      * instead of standard file from the library.

           OPEN INPUT TOPICFIL

           READ TOPICFIL

           INSPECT TOPIC-DATA-REC TALLYING TOPIC-LENGTH
                   FOR CHARACTERS BEFORE ' '

           MOVE FUNCTION TRIM(TOPIC-DATA-REC) TO
                      KAFKA-TOPIC-NAME(1:TOPIC-LENGTH)
           MOVE LOW-VALUE TO KAFKA-TOPIC-NAME(TOPIC-LENGTH + 1:)

           CLOSE TOPICFIL.

       INIT-KAFKA-CONSUMER.
      **************** Initialization section Begin *******************
      * Invoke the Consumer program to Initialize the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the Config file
      *****************************************************************
      * KAFKA-TYPE-PC is 0 for PRODUCER
      * KAFKA-TYPE-PC is 1 for CONSUMER
           MOVE 1               TO KAFKA-TYPE-PC
           MOVE PART-LIST-SIZE  TO KAFKA-PART-LIST-SIZE
           MOVE PART-VAL        TO PARTITION-VALUE
           MOVE MSGFLGS-VAL     TO MSGFLAGS-VALUE
           MOVE TIMEOUT-MS      TO TIMEOUT-MS-VALUE

           MOVE 'I'             TO KAFKA-ACTION
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
           END-IF.

      **************** Initialization section End *********************

       KAFKA-CONSUME-MESSAGE.
      **************** Consume section Begin **************************
      * The Events are retrieved from the Kafka by invoking the
      * consumer program using Kafka Action as 'C'
      *****************************************************************
           MOVE 'C'             TO KAFKA-ACTION
           DISPLAY "KAFKA CONSUME BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
                             RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
             IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT = -191
               MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
               MOVE 'Y' TO WS-END-CONSUMER
             ELSE
               MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
               MOVE 'Y' TO WS-END-CONSUMER
               DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
               DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
               MOVE 16 TO RETURN-CODE
             END-IF

           ELSE
             SET ADDRESS OF KAFKA-MSG-ASCII
                   TO KAFKA-PAYLOAD-64

             MOVE FUNCTION DISPLAY-OF (
                    FUNCTION
                       NATIONAL-OF(KAFKA-MSG-ASCII 819) 1047)
                         TO KAFKA-MSG-TEMP

             DISPLAY "MESSAGE CONSUMED : "
                               KAFKA-MSG-TEMP(1:KAFKA-PAYLOAD-LEN)
             DISPLAY "MESSAGE LENGTH : " KAFKA-PAYLOAD-LEN
             ADD 1 TO WS-CONSUME-CNT
             MOVE 'Y' TO WS-NEW-PARTITION-FLAG

             IF WS-RCNT > 0
               MOVE 1 TO WS-CNT1

               PERFORM UNTIL WS-CNT1 > WS-RCNT
                 IF WS-CHKPT-PARTITION(WS-CNT1) = PAYLOAD-PARTITION
                   MOVE PAYLOAD-OFFSET TO WS-CHKPT-OFFSET(WS-CNT1)
                   MOVE 'N' TO WS-NEW-PARTITION-FLAG
                 END-IF
                 ADD 1 TO WS-CNT1
               END-PERFORM
             END-IF

             IF WS-NEW-PARTITION-FLAG = 'Y'
               ADD 1 TO WS-RCNT
               MOVE WS-RCNT TO RESTART-PARTNOS
               MOVE PAYLOAD-OFFSET TO WS-CHKPT-OFFSET(WS-RCNT)
               MOVE PAYLOAD-PARTITION TO WS-CHKPT-PARTITION(WS-RCNT)
             END-IF

           END-IF

           DIVIDE WS-CONSUME-CNT BY 50 GIVING MC-QUOTIENT
                  REMAINDER MC-REMAINDER

           IF MC-REMAINDER = 0
             OPEN OUTPUT CHKPTFIL
             MOVE 1 TO WS-CNT1
             PERFORM UNTIL WS-CNT1 > WS-RCNT

               MOVE WS-CHKPT-OFFSET(WS-CNT1) TO CHKPT-OFFSET
               MOVE WS-CHKPT-PARTITION(WS-CNT1) TO CHKPT-PARTITION
               WRITE CHECK-POINT-FILE
               ADD 1 TO WS-CNT1
             END-PERFORM
             CLOSE CHKPTFIL

           END-IF
           .
      **************** Consume section End ****************************

       WRITE-CHKPT-FILE.
           OPEN OUTPUT CHKPTFIL
           MOVE 1 TO WS-CNT1
           PERFORM UNTIL WS-CNT1 > WS-RCNT

             MOVE WS-CHKPT-OFFSET(WS-CNT1) TO CHKPT-OFFSET
             MOVE WS-CHKPT-PARTITION(WS-CNT1) TO CHKPT-PARTITION
             WRITE CHECK-POINT-FILE
             ADD 1 TO WS-CNT1

           END-PERFORM

           CLOSE CHKPTFIL.

       DESTROY-KAFKA-CONSUME.
      **************** Deletion section Begin *************************
      * Delete the Kafka objects once all the messages are consumed
      *****************************************************************
           MOVE 'D'             TO KAFKA-ACTION
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
           END-IF.
      **************** Deletion section End ***************************

       END PROGRAM 'IXYCON64'.