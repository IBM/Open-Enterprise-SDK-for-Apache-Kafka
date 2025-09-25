       CBL LP(64)
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
      * KAFKA FUNCTION PROTOTYPES
      ******************************************************************
       COPY IXYPROTP.

      ******************************************************************
      * MESSAGE DELIVERY CALLBACK FUNCTION
      * This is a sample COBOL suprogram that can be used as message
      * delivery call back function.
      ******************************************************************
      * COPY IXYCBK1.

      ******************************************************************
      * MAIN PROGRAM PRODUCER
      ******************************************************************
      * This sample module produces the message passed to the KAFKA
      * topic provided for the KAFKA broker with SSL enabled.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYSPRDS'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
      ******************************************************************
      * KAFKA INPUT-OUTPUT DATA COPYBOOK
      ******************************************************************
         COPY IXYCOPY.
      ******************************************************************
      *  PRODUCER Values
      ******************************************************************
       >>DATA 31
         01 END-OF-STRING           PIC X(01) VALUE X'00'.
         01 INDEX-POS               PIC 9(04) BINARY VALUE 0.
         01 HOST-TEMP               PIC X(1024).
         01 VALUE-TEMP              PIC X(1024).
         01 WS-CNT                  PIC 9(2)  BINARY VALUE 1.
         01 ERR-STR                 PIC X(1024) VALUE SPACES.
         01 ERR-LEN                 PIC 9(18) BINARY VALUE 1024.
         01 ZERO-TIMEOUT            PIC 9(01) VALUE 0.
         01 MSG-CNT                 PIC 9(09) VALUE 0.
         01 MC-REMAINDER            PIC 9(04) VALUE 0.
         01 MC-QUOTIENT             PIC 9(04) VALUE 0.

       LINKAGE SECTION.
         01 PRODUCER-INPUT.
            COPY IXYPRDSI.
         01 PRODUCER-OUTPUT.
            COPY IXYPRDSO.
         01 KAFKA-VERSION           PIC X(1024).
         01 DATA-TEMP               PIC X(1024).

       PROCEDURE DIVISION USING PRODUCER-INPUT
           RETURNING PRODUCER-OUTPUT.

           EVALUATE TRUE
      * Kafka Initialisation
           WHEN KAFKA-INIT
             MOVE 1 TO WS-CNT

      * CALL KAFVER to find Kafka version
             MOVE FUNCTION IXY-KAFKA-VER
                   TO KAFKA-VER-PTR OF KAFKA-VERSION-STR-OUT
             SET ADDRESS OF KAFKA-VERSION
                   TO KAFKA-VER-PTR-31 OF KAFKA-VERSION-STR-OUT
             INSPECT KAFKA-VERSION TALLYING INDEX-POS FOR CHARACTERS
                             BEFORE INITIAL END-OF-STRING
             IF KAFKA-VER-PTR OF KAFKA-VERSION-STR-OUT NOT = 0
               DISPLAY "KAFKA VERSION IS : "
                   KAFKA-VERSION(1:INDEX-POS)
             ELSE
               MOVE "FAILED TO GET KAFKA VERSION" TO KAFKA-MSG
               MOVE 9001 TO KAFKA-MSG-RESPONSE
               GOBACK
             END-IF

      * Create KAFKA CONF pointer
             SET KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT
                             TO FUNCTION IXY-KAFKA-CONF-NEW

             IF KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT = NULL
               MOVE "FAILED TO CREATE KAFKA CONF NEW" TO KAFKA-MSG
               MOVE 9002 TO KAFKA-MSG-RESPONSE
               GOBACK
             END-IF

      * Create KAFKA SET Configuration
             SET KAFKA-CONF-REF OF KAFKA-CONF-SET-IN
                   TO KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT

      * Set all the Configuration properties
             PERFORM UNTIL WS-CNT > NUM-OF-PARMS

      * Convert Config name to ASCII
               SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                             TO ADDRESS OF CONFIG-NAME(WS-CNT)
               PERFORM CONVERT-EBC-ASC
               SET ADDRESS OF DATA-TEMP
                   TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
               MOVE DATA-TEMP TO HOST-TEMP
               SET PROP-NAME OF KAFKA-CONF-SET-IN
                             TO ADDRESS OF HOST-TEMP

      * Convert Config value to ASCII
               SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                             TO ADDRESS OF CONFIG-VALUE(WS-CNT)
               PERFORM CONVERT-EBC-ASC
               SET ADDRESS OF DATA-TEMP
                   TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
               MOVE DATA-TEMP TO VALUE-TEMP
               SET PROP-VALUE OF KAFKA-CONF-SET-IN
                             TO ADDRESS OF VALUE-TEMP

               INITIALIZE ERR-STR
               MOVE ERR-LEN   TO ERRSTR-SIZE OF KAFKA-CONF-SET-IN
               SET ERRSTR-PTR OF KAFKA-CONF-SET-IN TO
                             ADDRESS OF ERR-STR

               MOVE FUNCTION IXY-KAFKA-CONF-SET(
                             KAFKA-CONF-REF OF KAFKA-CONF-SET-IN
                             PROP-NAME      OF KAFKA-CONF-SET-IN
                             PROP-VALUE     OF KAFKA-CONF-SET-IN
                             ERRSTR-PTR     OF KAFKA-CONF-SET-IN
                             ERRSTR-SIZE    OF KAFKA-CONF-SET-IN
                             )
                             TO CONF-RES       OF KAFKA-CONF-SET-OUT

               IF CONF-RES NOT = 0
                 DISPLAY "**ERROR** : FAILURE FROM KAFKA-CONF-SET"
                 MOVE CONF-RES TO KAFKA-MSG-RESPONSE
                 PERFORM GENERATE-ERROR-STR-ASC-EBC
                 GOBACK
               END-IF

               ADD 1 TO WS-CNT
             END-PERFORM

      * Create CALLBACK function for Message delivery
             SET KAFKA-CALLBACK-REF OF KAFKA-CONF-SET-DR-MSG-CB-IN
                   TO ENTRY "IXYDLCB"
             SET KAFKA-CONF-REF OF KAFKA-CONF-SET-DR-MSG-CB-IN
                   TO KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT

             MOVE FUNCTION IXY-KAFKA-DELIVERY-MSG-CB(
                   KAFKA-CONF-REF OF KAFKA-CONF-SET-DR-MSG-CB-IN
                   KAFKA-CALLBACK-REF OF KAFKA-CONF-SET-DR-MSG-CB-IN
                   )
                   TO RETURN-STATUS OF KAFKA-CONF-SET-DR-MSG-CB-OUT

             IF RETURN-STATUS OF KAFKA-CONF-SET-DR-MSG-CB-OUT NOT = 0
               MOVE RETURN-STATUS OF KAFKA-CONF-SET-DR-MSG-CB-OUT TO
                             RETURN-STATUS    OF KAFKA-ERR2STR-IN
               PERFORM GENERATE-ERR-STR
               GOBACK
             END-IF

             IF DISABLE-LOG-CONV NOT = 'DISABLE-LOG-CONV'
      * Create CALLBACK function for Logging Error Messages
               SET KAFKA-CALLBACK-REF OF KAFKA-CONF-SET-LOG-CB-IN
                   TO ENTRY "IXYLGCB"
               SET KAFKA-CONF-REF OF KAFKA-CONF-SET-LOG-CB-IN
                   TO KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT

               MOVE FUNCTION IXY-KAFKA-CONF-SET-LOG-CB(
                   KAFKA-CONF-REF OF KAFKA-CONF-SET-LOG-CB-IN
                   KAFKA-CALLBACK-REF OF KAFKA-CONF-SET-LOG-CB-IN
                   )
                   TO RETURN-STATUS OF KAFKA-CONF-SET-LOG-CB-OUT

               IF RETURN-STATUS OF KAFKA-CONF-SET-LOG-CB-OUT NOT = 0
                 MOVE "LOG CALLBACK FAILURE" TO KAFKA-MSG
                 MOVE 9003 TO KAFKA-MSG-RESPONSE
                 GOBACK
               END-IF
             END-IF

      * Create PRODUCER
      * KAFKA-TYPE is 0 for PRODUCER
      * KAFKA-TYPE is 1 for CONSUMER
             MOVE KAFKA-TYPE-PC  TO KAFKA-TYPE     OF KAFKA-NEW-IN
             SET  KAFKA-CONF-REF OF KAFKA-NEW-IN
                             TO KAFKA-CONF-REF OF KAFKA-CONF-SET-IN

             INITIALIZE ERR-STR
             MOVE ERR-LEN   TO ERRSTR-SIZE OF KAFKA-NEW-IN
             SET ERRSTR-PTR OF KAFKA-NEW-IN TO
                             ADDRESS OF ERR-STR

             SET KAFKA-TYPE-REF OF KAFKA-NEW-OUT TO
                   FUNCTION IXY-KAFKA-NEW(
                             KAFKA-TYPE     OF KAFKA-NEW-IN
                             KAFKA-CONF-REF OF KAFKA-NEW-IN
                             ERRSTR-PTR     OF KAFKA-NEW-IN
                             ERRSTR-SIZE    OF KAFKA-NEW-IN
                             )

             IF KAFKA-TYPE-REF OF KAFKA-NEW-OUT = NULL
                MOVE 9004 TO KAFKA-MSG-RESPONSE
                PERFORM GENERATE-ERROR-STR-ASC-EBC
                GOBACK
             END-IF

      * Create TOPIC instance
      * Convert Topic to ASCII
             SET KAFKA-TYPE-REF OF KAFKA-TOPIC-NEW-IN
                             TO KAFKA-TYPE-REF OF KAFKA-NEW-OUT

             SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                             TO ADDRESS OF KAFKA-TOPIC-NAME

             PERFORM CONVERT-EBC-ASC

             SET KAFKA-TOPIC-PTR OF KAFKA-TOPIC-NEW-IN
                   TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
             SET KAFKA-TOPIC-CONF OF KAFKA-TOPIC-NEW-IN TO NULL

             SET KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT TO
                   FUNCTION IXY-KAFKA-TOPIC-NEW(
                             KAFKA-TYPE-REF   OF KAFKA-TOPIC-NEW-IN
                             KAFKA-TOPIC-PTR  OF KAFKA-TOPIC-NEW-IN
                             KAFKA-TOPIC-CONF OF KAFKA-TOPIC-NEW-IN
                             )

             IF KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT = NULL
               DISPLAY " **ERROR** : IXY-KAFKA-TOPIC-NEW FAILED"
               PERFORM GET-LAST-ERR
               MOVE RETURN-STATUS  OF KAFKA-LAST-ERROR-OUT TO
                             RETURN-STATUS    OF KAFKA-ERR2STR-IN
               PERFORM GENERATE-ERR-STR
               GOBACK
             ELSE
               MOVE 0 TO KAFKA-MSG-RESPONSE
               MOVE "KAFKA PRODUCER INIT SUCCESS" TO KAFKA-MSG
             END-IF

      * Create PRODUCE
           WHEN KAFKA-PRODUCE

             SET KAFKA-TOPIC-REF   OF KAFKA-PRODUCE-IN
                             TO KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT
             MOVE PARTITION-VALUE TO PARTITION OF KAFKA-PRODUCE-IN
             MOVE MSGFLAGS-VALUE  TO MSGFLAGS  OF KAFKA-PRODUCE-IN
             MOVE KAFKA-PAYLOAD-LEN TO PAYLOAD-LEN OF
                                               KAFKA-PRODUCE-IN

      * Convert Kafka payload into ASCII
             IF SKIP-CONV = 'Y'
               IF CALLER-31BIT = 'Y'
                 SET PAYLOAD OF KAFKA-PRODUCE-IN TO
                   KAFKA-PAYLOAD-31
               ELSE
                 SET PAYLOAD OF KAFKA-PRODUCE-IN TO
                   KAFKA-PAYLOAD
               END-IF  
             ELSE
               IF CALLER-31BIT = 'Y'
                 SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                                       TO KAFKA-PAYLOAD-31
               ELSE
                 SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                                       TO KAFKA-PAYLOAD
               END-IF                

               PERFORM CONVERT-EBC-ASC
               SET PAYLOAD OF KAFKA-PRODUCE-IN
                   TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
             END-IF

             SET PROD-KEY   OF KAFKA-PRODUCE-IN TO NULL
             MOVE 0         TO KEY-LEN OF KAFKA-PRODUCE-IN
             SET MSG-OPAQUE OF KAFKA-PRODUCE-IN TO NULL

             MOVE FUNCTION IXY-KAFKA-PRODUCE(
                             KAFKA-TOPIC-REF OF KAFKA-PRODUCE-IN
                             PARTITION       OF KAFKA-PRODUCE-IN
                             MSGFLAGS        OF KAFKA-PRODUCE-IN
                             PAYLOAD         OF KAFKA-PRODUCE-IN
                             PAYLOAD-LEN     OF KAFKA-PRODUCE-IN
                             PROD-KEY        OF KAFKA-PRODUCE-IN
                             KEY-LEN         OF KAFKA-PRODUCE-IN
                             MSG-OPAQUE      OF KAFKA-PRODUCE-IN
                             )
                             TO RETURN-STATUS   OF KAFKA-PRODUCE-OUT

             IF RETURN-STATUS  OF KAFKA-PRODUCE-OUT NOT = 0
               DISPLAY " **ERROR** : IXY-KAFKA-PRODUCE FAILED"
               PERFORM GET-LAST-ERR
      * Display response from Kafka Produce
               MOVE RETURN-STATUS  OF KAFKA-LAST-ERROR-OUT TO
                        RETURN-STATUS    OF KAFKA-ERR2STR-IN
               PERFORM GENERATE-ERR-STR
               GOBACK
             END-IF

             ADD 1 TO MSG-CNT

             DIVIDE MSG-CNT BY 1000 GIVING MC-QUOTIENT
                  REMAINDER MC-REMAINDER

      * Poll the queue for every 1000 messages
             IF MC-REMAINDER = 0 THEN
               SET KAFKA-TYPE-REF OF KAFKA-POLL-IN TO
                             KAFKA-TYPE-REF OF KAFKA-NEW-OUT
               MOVE ZERO-TIMEOUT TO
                        TIMEOUT-MS      OF KAFKA-POLL-IN
               MOVE FUNCTION IXY-KAFKA-POLL(
                               KAFKA-TYPE-REF  OF KAFKA-POLL-IN
                               TIMEOUT-MS      OF KAFKA-POLL-IN
                               )
                               TO EVENT-CNT OF KAFKA-POLL-OUT
               DISPLAY "EVENT-CNT FROM POLL : " EVENT-CNT
             END-IF
             MOVE 0 TO KAFKA-MSG-RESPONSE
             MOVE "KAFKA PRODUCE DONE" TO KAFKA-MSG

      * Clean-up
           WHEN KAFKA-DELETE
      * Flush the queue
             SET KAFKA-TYPE-REF OF KAFKA-FLUSH-IN TO
                             KAFKA-TYPE-REF OF KAFKA-NEW-OUT
             MOVE TIMEOUT-MS-VALUE TO
                             TIMEOUT-MS      OF KAFKA-FLUSH-IN

             MOVE FUNCTION IXY-KAFKA-FLUSH(
                             KAFKA-TYPE-REF  OF KAFKA-FLUSH-IN
                             TIMEOUT-MS      OF KAFKA-FLUSH-IN
                             )
                             TO RETURN-STATUS   OF KAFKA-FLUSH-OUT

             IF RETURN-STATUS  OF KAFKA-FLUSH-OUT NOT = 0
                DISPLAY " **ERROR** : IXY-KAFKA-FLUSH FAILED"
                MOVE RETURN-STATUS  OF KAFKA-FLUSH-OUT TO
                             RETURN-STATUS    OF KAFKA-ERR2STR-IN
                PERFORM GENERATE-ERR-STR
                GOBACK
             END-IF

             SET KAFKA-TOPIC-REF OF KAFKA-TOPIC-DESTROY-IN TO
                             KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT

             MOVE FUNCTION IXY-KAFKA-TOPIC-DESTROY(
                   KAFKA-TOPIC-REF   OF KAFKA-TOPIC-DESTROY-IN
                   ) TO
                   RETURN-STATUS OF KAFKA-TOPIC-DESTROY-OUT

             IF RETURN-STATUS OF KAFKA-TOPIC-DESTROY-OUT NOT = 0
               DISPLAY "VALUE RETURNED FROM KAFTDEST : "
                             RETURN-STATUS OF KAFKA-TOPIC-DESTROY-OUT
               MOVE "FAILURE IN TOPIC-DESTROY" TO KAFKA-MSG
               MOVE 9005 TO KAFKA-MSG-RESPONSE
               GOBACK
             END-IF

             SET KAFKA-TYPE-REF OF KAFKA-DESTROY-IN TO
                            KAFKA-TYPE-REF  OF KAFKA-NEW-OUT

             MOVE FUNCTION IXY-KAFKA-DESTROY(
                            KAFKA-TYPE-REF  OF KAFKA-DESTROY-IN
                            )
                       TO RETURN-STATUS      OF KAFKA-DESTROY-OUT

             IF RETURN-STATUS OF KAFKA-TOPIC-DESTROY-OUT NOT = 0
               DISPLAY "VALUE RETURNED FROM KAFDEST :  "
                             RETURN-STATUS      OF KAFKA-DESTROY-OUT
               MOVE "FAILURE IN KAFKA-DESTROY" TO KAFKA-MSG
               MOVE 9006 TO KAFKA-MSG-RESPONSE
               GOBACK
             ELSE
               MOVE 0 TO KAFKA-MSG-RESPONSE
               MOVE "KAFKA PRODUCER DESTROY DONE" TO KAFKA-MSG
             END-IF

           WHEN OTHER

             MOVE "INVALID KAFKA-ACTION FOR PRODUCER"
               TO KAFKA-MSG
             MOVE 9999 TO KAFKA-MSG-RESPONSE
             GOBACK
           END-EVALUATE.

           GOBACK.

       CONVERT-EBC-ASC.
           MOVE FUNCTION EBCDIC-ASCII-CONV(
                   EBCDIC-DATA-PTR OF EBCDIC-ASCII-CONV-IN
                   ) TO
                   ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT.

           IF ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT = 0
             MOVE "FAILURE WHILE CONVERTING EBSIDIC DATA TO ASCII"
                                                 TO KAFKA-MSG
             MOVE 9007 TO KAFKA-MSG-RESPONSE
             GOBACK
           END-IF.

       GENERATE-ERROR-STR-ASC-EBC.

           SET ASCII-DATA-PTR-31 OF ASCII-EBCDIC-CONV-IN
                 TO ADDRESS OF ERR-STR

           MOVE FUNCTION ASCII-EBCDIC-CONV(
                 ASCII-DATA-PTR OF ASCII-EBCDIC-CONV-IN
                 ) TO
                 EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT

           IF EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT = 0
             MOVE "FAILURE WHILE CONVERTING ASCII DATA TO EBSIDIC"
                                                 TO KAFKA-MSG
             MOVE 9008 TO KAFKA-MSG-RESPONSE
             GOBACK
           END-IF

           SET ADDRESS OF DATA-TEMP
                 TO EBCDIC-DATA-PTR-31 OF ASCII-EBCDIC-CONV-OUT
           MOVE 0 TO INDEX-POS

           INSPECT DATA-TEMP TALLYING INDEX-POS FOR CHARACTERS
                 BEFORE INITIAL END-OF-STRING

           MOVE DATA-TEMP(1:INDEX-POS) TO KAFKA-MSG.

       GET-LAST-ERR.

           MOVE FUNCTION IXY-KAFKA-LAST-ERROR
                 TO RETURN-STATUS OF KAFKA-LAST-ERROR-OUT

           IF RETURN-STATUS OF KAFKA-LAST-ERROR-OUT = 0
             MOVE "FAILURE WHILE GETTING LAST ERROR"
                                                 TO KAFKA-MSG
             MOVE 9009 TO KAFKA-MSG-RESPONSE
             GOBACK
           END-IF.

       GENERATE-ERR-STR.

           MOVE FUNCTION IXY-KAFKA-ERR2STR(
                 RETURN-STATUS    OF KAFKA-ERR2STR-IN
                 )
                 TO KAFKA-E2S-EBC-PTR OF KAFKA-ERR2STR-OUT

           IF KAFKA-E2S-EBC-PTR OF KAFKA-ERR2STR-OUT = 0
             MOVE "FAILURE WHILE CONVERTING ERROR TO STRING"
                                                 TO KAFKA-MSG
             MOVE 9010 TO KAFKA-MSG-RESPONSE
             GOBACK
           END-IF

           SET ADDRESS OF DATA-TEMP
                 TO KAFKA-E2S-EBC-PTR-31 OF KAFKA-ERR2STR-OUT
           MOVE 0 TO INDEX-POS

           INSPECT DATA-TEMP TALLYING INDEX-POS FOR CHARACTERS
                 BEFORE INITIAL END-OF-STRING

           MOVE DATA-TEMP(1:INDEX-POS) TO KAFKA-MSG
           MOVE RETURN-STATUS OF KAFKA-ERR2STR-IN TO
                                       KAFKA-MSG-RESPONSE.

       END PROGRAM 'IXYSPRDS'.
