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
      * MAIN PROGRAM IXYPRO31
      ******************************************************************
      * This is a sample program compiled in 31 bit addressing mode.
      * This program calls IXYSPRDS module and produce the message
      * passed on from SYSIN of IXYJRO31 JCL.
      *
      * The program should be modified with the following changes:
      * 1) @@HOST_VALUE@@ - This should be changed to the KAFKA
      *    broker.  The length of the variable should be
      *    adjusted to the length of the KAFKA broker value.
      * 2) The value of PART-VAL should be set to the target partition
      *    value.
      * 3) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 4) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events
      * 5) Around 15 Configuration Parameters can be passed. Need to
      *    update The NUM-OF-PARMS value accordingly.
      * 6) Other Configuration Parameters can be coded similar to
      *    the HOST and its value. Length has to be altered accordingly.
      * 7) Topic Data is passed as a PARM parameter.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYPRO31'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
      ******************************************************************
      *  PRODUCER Values
      ******************************************************************
         01 KAFKA-HOST-E.
            05 PROP-NAME.
               10 FILLER        PIC X(17) VALUE 'bootstrap.servers'.
               10 FILLER        PIC X(01) VALUE X'00'.
            05 PROP-VAL.
               10 FILLER        PIC X(14)
                  VALUE '@@HOST_VALUE@@'.
               10 FILLER        PIC X(01) VALUE X'00'.

         01 KAFKA-TOPIC-E.
            05 KAFKA-TOPIC      PIC X(04).
            05 FILLER           PIC X(01) VALUE X'00'.
         01 PART-VAL            PIC S9(9) BINARY VALUE -1.
         01 KAFKA-MSG-LEN       PIC S9(18) BINARY.
         01 MSGFLGS-VAL         PIC X(01) VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9) BINARY VALUE 5000.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
         01 KAFKA-MSG-PAYLOAD   PIC X(1024).

      * Input values for Producer program
         01 PRODUCER-INPUT.
            COPY IXYPRDSI.
      * Output values for Producer program
         01 PRODUCER-OUTPUT.
            COPY IXYPRDSO.
         01 PRODUCER-PGM        PIC X(8) VALUE "IXYSPRDS".

        LINKAGE SECTION.
         01 PARM-DATA.
           05 PARM-LENGTH           PIC S9(4) COMP.
           05 TOPIC-NAME            PIC X(4).
           05 PARM-DISABLE-LOG-CONV  PIC X(16).

       PROCEDURE DIVISION USING PARM-DATA.
           DISPLAY "KAFKA AMODE 31 PROGRAM"
           ACCEPT KAFKA-MSG-PAYLOAD
           COMPUTE KAFKA-MSG-LEN =
             FUNCTION LENGTH(FUNCTION TRIM(KAFKA-MSG-PAYLOAD))

           MOVE TOPIC-NAME TO KAFKA-TOPIC
      **************** Initialisation section Begin *******************
      * Invoke the Producer program to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are set in the program.
      *****************************************************************
           MOVE PROP-NAME OF KAFKA-HOST-E
                                TO CONFIG-NAME(1)
           MOVE PROP-VAL  OF KAFKA-HOST-E
                                TO CONFIG-VALUE(1)

           MOVE 1               TO NUM-OF-PARMS

           MOVE KAFKA-TOPIC-E   TO KAFKA-TOPIC-NAME
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
      **************** Initialisation section End *********************

      **************** Producer section Begin *************************
      * The Events are retrieved from the SYSIN and Producer
      * Program is invoked for each event to Produce the Kafka event
      *****************************************************************
           SET KAFKA-PAYLOAD-31 TO ADDRESS OF KAFKA-MSG-PAYLOAD
           MOVE KAFKA-MSG-LEN   TO KAFKA-PAYLOAD-LEN
           MOVE 'P'             TO KAFKA-ACTION
           MOVE 'Y' TO CALLER-31BIT
           DISPLAY "KAFKA PRODUCE BEGIN"
           DISPLAY "Message To be Produced : " 
                KAFKA-MSG-PAYLOAD(1:KAFKA-PAYLOAD-LEN)

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
       END PROGRAM 'IXYPRO31'.