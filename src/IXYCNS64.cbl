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
      * MAIN PROGRAM IXYCNS64
      ******************************************************************
      * This is a sample program compiled in 64 bit addressing mode.
      * This program calls IXYSCONS module and consume the message
      * from kafka.
      *
      * The program should be modified with the following changes:
      * 1) @@HOST_VALUE@@ - This should be changed to the KAFKA
      *    broker.  The length of the variable should be
      *    adjusted to the length of the KAFKA broker value.
      * 2) The value of PART-VAL should be set to the target partition
      *    value.
      * 3) The value of PART-LIST-SIZE should be set to the size of
      *    topic partition list.
      * 4) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 5) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events
      * 6) Around 15 Configuration Parameters can be passed. Need to
      *    update the NUM-OF-PARMS value accordingly.
      * 7) Other Configuration Parameters can be coded similar to
      *    the HOST and its value. Length has to be altered accordingly.
      * 8) Topic Data is passed as a PARM parameter.
      * 9) @@GROUP_ID@@ - Replace this with group.id value and adjust 
      *    the length of variable accordingly.     
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYCNS64'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
      ******************************************************************
      *  CONSUMER Values
      ******************************************************************
         01 KAFKA-HOST-E.
            05 PROP-NAME.
               10 FILLER        PIC X(17) VALUE 'bootstrap.servers'.
               10 FILLER        PIC X(01) VALUE X'00'.
            05 PROP-VAL.
               10 FILLER        PIC X(14)
                  VALUE '@@HOST_VALUE@@'.
               10 FILLER        PIC X(01) VALUE X'00'.
        01 KAFKA-GRP-E.
            05 PROP-NAME.
               10 FILLER        PIC X(8) VALUE 'group.id'.
               10 FILLER        PIC X(01) VALUE X'00'.
            05 PROP-VAL.
               10 FILLER        PIC X(12)  VALUE '@@GROUP_ID@@'.
               10 FILLER        PIC X(01) VALUE X'00'.
         01 KAFKA-AUTO-E.
            05 PROP-NAME.
               10 FILLER        PIC X(17) VALUE 'auto.offset.reset'.
               10 FILLER        PIC X(01) VALUE X'00'.
            05 PROP-VAL.
               10 FILLER        PIC X(8)
                  VALUE 'earliest'.
               10 FILLER        PIC X(01) VALUE X'00'.

         01 KAFKA-TOPIC-E.
            05 KAFKA-TOPIC      PIC X(04).
            05 FILLER           PIC X(01)  VALUE X'00'.
         01 PART-VAL            PIC S9(9)  BINARY VALUE -1.
         01 PART-LIST-SIZE      PIC S9(09) BINARY VALUE 1.
         01 MSGFLGS-VAL         PIC X(01)  VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9)  BINARY VALUE 9999.
         01 WS-END-CONSUMER     PIC X(1)   VALUE 'N'.
         01 WS-CONSUME-CNT      PIC 9(9)   VALUE 0.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
         01 KAFKA-MSG-TEMP      PIC X(1024).

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
         01 TOPIC-DATA.
           05 TOPIC-LENGTH      PIC S9(4) COMP.
           05 TOPIC-NAME        PIC X(4).
         01 KAFKA-MSG-ASCII          PIC X(1024).

       PROCEDURE DIVISION USING TOPIC-DATA.
           DISPLAY "KAFKA AMODE 31 PROGRAM"
           MOVE TOPIC-NAME TO KAFKA-TOPIC

      **************** Initialisation section Begin *******************
      * Invoke the Consumer program to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are set in the program.
      *****************************************************************
           MOVE PROP-NAME OF KAFKA-HOST-E
                                TO CONFIG-NAME(1)
           MOVE PROP-VAL  OF KAFKA-HOST-E
                                TO CONFIG-VALUE(1)
           MOVE PROP-NAME OF KAFKA-GRP-E
                                TO CONFIG-NAME(2)
           MOVE PROP-VAL  OF KAFKA-GRP-E
                                TO CONFIG-VALUE(2)
           MOVE PROP-NAME OF KAFKA-AUTO-E
                                TO CONFIG-NAME(3)
           MOVE PROP-VAL  OF KAFKA-AUTO-E
                                TO CONFIG-VALUE(3)
           MOVE 3               TO NUM-OF-PARMS
           MOVE KAFKA-TOPIC-E   TO KAFKA-TOPIC-NAME
           MOVE PART-LIST-SIZE  TO KAFKA-PART-LIST-SIZE
           MOVE 1               TO KAFKA-TYPE-PC
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
           END-IF
      **************** Initialisation section End *********************
      **************** Consume section Begin **************************
      * The Events are retrieved from the kafka by invoking the
      * consumer prgram using Kafka Action as 'C'
      *****************************************************************
           MOVE 'C'             TO KAFKA-ACTION
           DISPLAY "KAFKA CONSUME BEGIN"

           PERFORM UNTIL WS-END-CONSUMER = 'Y'

             CALL CONSUMER-PGM USING CONSUMER-INPUT
                             RETURNING CONSUMER-OUTPUT

             IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
               MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
               MOVE 'Y' TO WS-END-CONSUMER
               IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT = -191 OR
                 (KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT = -185 AND
                  WS-CONSUME-CNT > 0)
                 DISPLAY "WARNING : " FUNCTION TRIM(KAFKA-MSG)
                 DISPLAY "WARNING CODE : " WS-DISPLAY-ERR
               ELSE
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
             END-IF
           END-PERFORM
           DISPLAY "KAFKA CONSUME DONE"
           DISPLAY "NUMBER OF KAFKA MESSAGES CONSUMED : " WS-CONSUME-CNT
      **************** Consume section End ****************************
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
           END-IF
      **************** Deletion section End ***************************
           GOBACK.
       END PROGRAM 'IXYCNS64'.