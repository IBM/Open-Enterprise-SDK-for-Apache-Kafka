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
      * MAIN PROGRAM IXYPRD31
      ******************************************************************
      * This is a sample program compiled in 31 bit addressing mode.
      * This program calls IXYSPRDS module and produce the message
      * passed on from EVENTFIL. This program uses CONFFILE to get the
      * Configuration parameters needed. This program also uses
      * TOPICFIL to get the topic details.
      *
      * The program should be modified with the following changes:
      * 1) The value of PART-VAL should be set to the target partition
      *    value.
      * 2) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 3) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events.
      * 4) EVENTFIL - This is the File which contains the kafka messages
      *    to be produced. Change the structure, file description
      *    and use a different flat file instead of standard file from
      *    the library accordingly, if the length is more than 1024.
      *    This file will be read and each record data is produced as a
      *    kafka event.
      * 5) TOPICFIL - This is the file which contains the topic details.
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the topic length crosses 1024 bytes.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYPRD31'.
       ENVIRONMENT DIVISION.
        INPUT-OUTPUT SECTION.
         FILE-CONTROL.
           SELECT CONFFILE ASSIGN TO CONFFILE
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT EVENTFIL ASSIGN TO EVENTFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT TOPICFIL ASSIGN TO TOPICFIL
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
            05 KAFKA-CONFIG-REC   PIC X(2049).

         FD EVENTFIL
           RECORD CONTAINS 1024  CHARACTERS
           BLOCK  CONTAINS 10240 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  EVENT-DATA.

         01 EVENT-DATA.
            05 EVENT-DATA-REC     PIC X(1024).

         FD TOPICFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  TOPIC-DATA.

         01 TOPIC-DATA.
            05 TOPIC-DATA-REC     PIC X(2049).

        WORKING-STORAGE SECTION.
         01 TOPIC-LENGTH        PIC S9(4) BINARY VALUE 0000.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
         01 PART-VAL            PIC S9(9) BINARY VALUE -1.
         01 MSGFLGS-VAL         PIC X(01) VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9) BINARY VALUE 5000.
         01 WS-MSG-BUF          PIC X(1024).

      * File Status
         01 WS-FILE-STATUS      PIC 9(02).
         01 WS-EOF-SW           PIC X(01).
             88 WS-EOF          VALUE 'Y'.
             88 WS-NOT-EOF      VALUE 'N'.

      * Configuration file
         01 WS-CNT              PIC S9(9) BINARY VALUE 0000.
         01 WS-PARMLEN          PIC S9(9) BINARY VALUE 0000.
         01 WS-VALLEN           PIC S9(9) BINARY VALUE 0000.
         01 WS-MSGLEN           PIC S9(9) BINARY VALUE 0000.
         01 WS-DELIMITER-POS    PIC S9(9) BINARY VALUE 0000.

         01 KAFKA-CONFIG-DATA.
            05 KAFKA-CONFIG-PARM      PIC X(1024).
            05 WS-DELIMITER           PIC X VALUE '='.
            05 KAFKA-CONFIG-VALUE     PIC X(1024).

      * Input values for Producer program
         01 PRODUCER-INPUT.
            COPY IXYPRDSI.
      * Output values for Producer program
         01 PRODUCER-OUTPUT.
            COPY IXYPRDSO.
         01 PRODUCER-PGM        PIC X(8) VALUE "IXYSPRDS".

       LINKAGE SECTION.
         01 PARM-DATA.
           05 PARM-LENGTH            PIC S9(4) COMP.
           05 PARM-DISABLE-LOG-CONV  PIC X(16).

       PROCEDURE DIVISION USING PARM-DATA .
           DISPLAY "KAFKA AMODE 31 PRODUCER MAIN PROGRAM"

      * CONFFILE contains the Configuration Parameters which are needed
      * for setting up the KAFKA connection. Configuration file is read
      * and parsed to extract the configuration Parameter and its
      * value. Length of Configuration Parameter and its value is
      * determined. End of string (LOW VALUES) is appended to the
      * configuration parameter and value. This file can contain
      * comments starting with '#'. Parameter and Value is delimited
      * by '='.

           OPEN INPUT CONFFILE
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

           CLOSE CONFFILE

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

           CLOSE TOPICFIL

      **************** Initialization section Begin *******************
      * Invoke the Producer program to Initialize the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the Config file
      *****************************************************************

      * KAFKA-TYPE-PC is 0 for PRODUCER
      * KAFKA-TYPE-PC is 1 for CONSUMER
           MOVE 0               TO KAFKA-TYPE-PC
           MOVE PART-VAL        TO PARTITION-VALUE
           MOVE MSGFLGS-VAL     TO MSGFLAGS-VALUE
           MOVE TIMEOUT-MS      TO TIMEOUT-MS-VALUE
           MOVE 'I'             TO KAFKA-ACTION
           MOVE PARM-DISABLE-LOG-CONV TO DISABLE-LOG-CONV
           DISPLAY "KAFKA PRODUCER INIT BEGIN"

           CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
             MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG)
           END-IF
      **************** Initialization section End *********************

      **************** Producer section Begin *************************
      * The Events are retrieved from the Event file and Producer
      * Program is invoked for each event to Produce the Kafka event
      *****************************************************************
           OPEN INPUT EVENTFIL
           SET WS-NOT-EOF TO TRUE
           MOVE 0 TO WS-CNT
           PERFORM UNTIL WS-EOF

             READ EVENTFIL
             AT END SET WS-EOF TO TRUE
             NOT AT END
               MOVE EVENT-DATA TO WS-MSG-BUF
               COMPUTE WS-MSGLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(WS-MSG-BUF TRAILING))
               SET KAFKA-PAYLOAD-31 TO ADDRESS OF WS-MSG-BUF
               MOVE 'Y' TO CALLER-31BIT
               MOVE WS-MSGLEN TO KAFKA-PAYLOAD-LEN
               MOVE 'P'        TO KAFKA-ACTION
               DISPLAY "KAFKA PRODUCE BEGIN"
               DISPLAY "MESSAGE TO BE PRODUCED : "
                                    WS-MSG-BUF(1:WS-MSGLEN)

               CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

               IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
                 DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
                 MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
                 DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
                 MOVE 16 TO RETURN-CODE
               ELSE
                 ADD 1 TO WS-CNT
                 DISPLAY FUNCTION TRIM(KAFKA-MSG)
               END-IF
             END-READ
           END-PERFORM

           CLOSE EVENTFIL

           DISPLAY "NUMBER OF KAFKA MESSAGES PRODUCED : " WS-CNT
      **************** Producer section End ***************************
      **************** Deletion section Begin *************************
      * Delete the Kafka objects once all the messages are produced
      *****************************************************************
           MOVE 'D'             TO KAFKA-ACTION
           DISPLAY "KAFKA PRODUCER DESTROY BEGIN"

           CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG)
             MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG)
           END-IF
      **************** Deletion section End ***************************
           GOBACK
             .
       END PROGRAM 'IXYPRD31'.