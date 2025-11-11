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
      * MAIN PROGRAM IXYSCONS
      ******************************************************************
      * This sample module consumes the message from the KAFKA topic
      * provided for the KAFKA broker.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYSCONS'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
      ******************************************************************
      * KAFKA INPUT-OUTPUT DATA COPYBOOK
      ******************************************************************
         COPY IXYCOPY.
      ******************************************************************
      *  CONSUMER Values
      ******************************************************************
       01 CONV-INPUT.
            COPY IXYCONVI.
       01 CONV-OUTPUT.
            COPY IXYCONVO.
       01 CONV-PGM         PIC X(8)    VALUE "IXYSCONV".
       >>DATA 31
       01 END-OF-STRING    PIC X(01)   VALUE X'00'.
       01 INDEX-POS        PIC 9(04) BINARY
                                       VALUE 0.
       01 WS-CNT           PIC 9(2) BINARY
                                       VALUE 1.
       01 WS-RCNT          PIC 9(2) BINARY
                                       VALUE 1.

       01 HOST-TEMP        PIC X(1024).
       01 VALUE-TEMP       PIC X(1024).

       01 ERR-STR          PIC X(1024) VALUE SPACES.
       01 ERR-LEN          PIC 9(18) BINARY
                                       VALUE 1024.

       LINKAGE SECTION.
         COPY IXYMESSG.
       01 CONSUMER-INPUT.
            COPY IXYCONSI.
       01 CONSUMER-OUTPUT.
            COPY IXYCONSO.
       01 DATA-TEMP        PIC X(1024).

       PROCEDURE DIVISION USING CONSUMER-INPUT
           RETURNING CONSUMER-OUTPUT.

           INITIALIZE CONSUMER-OUTPUT

           EVALUATE TRUE
           WHEN KAFKA-INIT
      ******************************************************************
      * The KAFKA-INIT block initializes the Kafka consumer environment.
      * It begins by creating a new Kafka configuration object using
      * IXY-KAFKA-CONF-NEW. The configuration is applied using
      * IXY-KAFKA-CONF-SET. If logging is enabled, a callback function
      * (IXYLGCB) is registered using IXY-KAFKA-CONF-SET-LOG-CB.
      * Next, the Kafka consumer is created using IXY-KAFKA-NEW, passing
      * the configuration reference and error handling structures.
      * The topic name is converted from EBCDIC to ASCII, and the topic
      * is created using IXY-KAFKA-TOPIC-NEW. Then initiate Kafka
      * message consumption from specific partitions and offsets. For
      * each partition, it sets the partition number and the
      * corresponding offset in the input structure. The
      * IXY-KAFKA-CONSUME-START function is then invoked to begin
      * consumption from the specified offset. This block ensures
      * complete setup of the Kafka consumer, including configuration,
      * logging, and topic initialization and setting up to consume from
      * particular partitions and offsets with robust error handling
      * throughout.
      ******************************************************************
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
                        MOVE ERR-LEN TO ERRSTR-SIZE OF KAFKA-CONF-SET-IN
                        SET ERRSTR-PTR OF KAFKA-CONF-SET-IN TO
                           ADDRESS OF ERR-STR

                        MOVE FUNCTION IXY-KAFKA-CONF-SET
                           (
                           KAFKA-CONF-REF OF KAFKA-CONF-SET-IN
                           PROP-NAME OF KAFKA-CONF-SET-IN
                           PROP-VALUE OF KAFKA-CONF-SET-IN
                           ERRSTR-PTR OF KAFKA-CONF-SET-IN
                           ERRSTR-SIZE OF KAFKA-CONF-SET-IN
                           )
                           TO CONF-RES OF KAFKA-CONF-SET-OUT

                        IF CONF-RES NOT = 0
                           DISPLAY
                              "**ERROR** : FAILURE FROM KAFKA-CONF-SET"
                           MOVE CONF-RES TO KAFKA-MSG-RESPONSE
                           SET ASCII-DATA-PTR-31 OF ASCII-EBCDIC-CONV-IN
                              TO ADDRESS OF ERR-STR
                           PERFORM CONVERT-ASC-EBC
                           SET ADDRESS OF DATA-TEMP
                              TO EBCDIC-DATA-PTR-31 OF
                              ASCII-EBCDIC-CONV-OUT
                           MOVE 0 TO INDEX-POS

                           INSPECT DATA-TEMP TALLYING INDEX-POS FOR
                              CHARACTERS
                              BEFORE INITIAL END-OF-STRING

                           MOVE DATA-TEMP(1:INDEX-POS) TO KAFKA-MSG
                           GOBACK
                        END-IF

                        ADD 1 TO WS-CNT
                END-PERFORM

                IF DISABLE-LOG-CONV NOT = 'DISABLE-LOG-CONV'
      * Create CALLBACK function for Logging Error Messages
                   SET KAFKA-CALLBACK-REF OF KAFKA-CONF-SET-LOG-CB-IN
                      TO ENTRY "IXYLGCB"
                   SET KAFKA-CONF-REF OF KAFKA-CONF-SET-LOG-CB-IN
                      TO KAFKA-CONF-REF OF KAFKA-CONF-NEW-OUT

                   MOVE FUNCTION IXY-KAFKA-CONF-SET-LOG-CB
                      (
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

      * Create CONSUMER
      * KAFKA-TYPE is 0 for PRODUCER
      * KAFKA-TYPE is 1 for CONSUMER
                MOVE KAFKA-TYPE-PC TO KAFKA-TYPE OF KAFKA-NEW-IN
                SET KAFKA-CONF-REF OF KAFKA-NEW-IN
                   TO KAFKA-CONF-REF OF KAFKA-CONF-SET-IN
                MOVE ERR-LEN TO ERRSTR-SIZE OF KAFKA-NEW-IN
                SET ERRSTR-PTR OF KAFKA-NEW-IN TO
                   ADDRESS OF ERR-STR
                SET KAFKA-TYPE-REF OF KAFKA-NEW-OUT TO
                   FUNCTION IXY-KAFKA-NEW(
                   KAFKA-TYPE OF KAFKA-NEW-IN
                   KAFKA-CONF-REF OF KAFKA-NEW-IN
                   ERRSTR-PTR OF KAFKA-NEW-IN
                   ERRSTR-SIZE OF KAFKA-NEW-IN
                   )

                IF KAFKA-TYPE-REF OF KAFKA-NEW-OUT = NULL
                   MOVE 9004 TO KAFKA-MSG-RESPONSE
                   SET ASCII-DATA-PTR-31 OF ASCII-EBCDIC-CONV-IN
                      TO ADDRESS OF ERR-STR
                   PERFORM CONVERT-ASC-EBC
                   SET ADDRESS OF DATA-TEMP
                      TO EBCDIC-DATA-PTR-31 OF ASCII-EBCDIC-CONV-OUT
                   MOVE 0 TO INDEX-POS

                   INSPECT DATA-TEMP TALLYING INDEX-POS FOR CHARACTERS
                      BEFORE INITIAL END-OF-STRING

                   MOVE DATA-TEMP(1:INDEX-POS) TO KAFKA-MSG
                   GOBACK
                END-IF

                SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                   TO ADDRESS OF KAFKA-TOPIC-NAME
                PERFORM CONVERT-EBC-ASC
                SET KAFKA-TOPIC-PTR OF KAFKA-TOPIC-NEW-IN
                   TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
                SET KAFKA-TOPIC-CONF OF KAFKA-TOPIC-NEW-IN TO NULL
                SET KAFKA-TYPE-REF OF KAFKA-TOPIC-NEW-IN
                   TO KAFKA-TYPE-REF OF KAFKA-NEW-OUT

                SET KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT TO
                   FUNCTION IXY-KAFKA-TOPIC-NEW(
                   KAFKA-TYPE-REF OF KAFKA-TOPIC-NEW-IN
                   KAFKA-TOPIC-PTR OF KAFKA-TOPIC-NEW-IN
                   KAFKA-TOPIC-CONF OF KAFKA-TOPIC-NEW-IN
                   )

                IF KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT = NULL
                   DISPLAY " **ERROR** : IXY-KAFKA-TOPIC-NEW FAILED"
                   PERFORM GET-LAST-ERR
                   MOVE RETURN-STATUS OF KAFKA-LAST-ERROR-OUT TO
                      RETURN-STATUS OF KAFKA-ERR2STR-IN
                   PERFORM GENERATE-ERR-STR
                   GOBACK
                END-IF

                SET KAFKA-TOPIC-REF OF KAFKA-CONSUME-START-IN TO
                   KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT

                PERFORM UNTIL WS-RCNT > TOTAL-PARTNOS

                        MOVE RESTART-PARTITION(WS-RCNT) TO PARTITION OF
                           KAFKA-CONSUME-START-IN

                        MOVE RESTART-OFFSET(WS-RCNT) TO OFFSET OF
                           KAFKA-CONSUME-START-IN

                        MOVE FUNCTION IXY-KAFKA-CONSUME-START
                           (
                           KAFKA-TOPIC-REF OF KAFKA-CONSUME-START-IN
                           PARTITION OF KAFKA-CONSUME-START-IN
                           OFFSET OF KAFKA-CONSUME-START-IN
                           )
                           TO RETURN-STATUS OF KAFKA-CONSUME-START-OUT

                        IF RETURN-STATUS OF KAFKA-CONSUME-START-OUT
                           NOT = 0
                           MOVE RETURN-STATUS OF KAFKA-LAST-ERROR-OUT TO
                              RETURN-STATUS OF KAFKA-ERR2STR-IN

                           PERFORM GENERATE-ERR-STR
                           GOBACK
                        END-IF
                        ADD 1 TO WS-RCNT
                END-PERFORM

                MOVE "KAFKA CONSUMER INIT DONE" TO KAFKA-MSG
                MOVE 0 TO KAFKA-MSG-RESPONSE

           WHEN KAFKA-CONSUME
      ******************************************************************
      * The KAFKA-CONSUME block is responsible for consuming a message
      * from a Kafka topic partition. The IXY-KAFKA-CONSUME function is
      * invoked to retrieve a message. If a valid message is received
      * and no error is reported, the message, its length, partition and
      * offset are extracted. Depending on whether the caller is
      * operating in 31-bit mode, the message payload is either
      * converted using a separate conversion program (IXYSCONV) or
      * directly assigned. If the conversion fails, an error message and
      * code are set, and the program exits. If no repsonse obtained
      * from IXY-KAFKA-CONSUME, then using IXY-KAFKA-LAST-ERROR retrieve
      * the latest error code, get the error message generated and exit. 
      * If end of partition error obtained i.e., -191, then set error 
      * code and error description only. Destroy the message finally
      * using IXY-KAFKA-MESSAGE-DESTROY.
      ******************************************************************
                SET KAFKA-TOPIC-REF OF KAFKA-CONSUME-IN
                   TO KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT
                MOVE TIMEOUT-MS-VALUE TO
                   TIMEOUT-MS OF KAFKA-CONSUME-IN
                MOVE CONSUME-PARTITION TO PARTITION
                   OF KAFKA-CONSUME-IN
                SET KAFKA-MESSAGE-REF OF KAFKA-CONSUME-OUT TO
                   FUNCTION IXY-KAFKA-CONSUME(
                   KAFKA-TOPIC-REF OF KAFKA-CONSUME-IN
                   PARTITION OF KAFKA-CONSUME-IN
                   TIMEOUT-MS OF KAFKA-CONSUME-IN
                   )

                SET ADDRESS OF KAFKA-MESSAGE TO
                   KAFKA-MESSAGE-REF OF KAFKA-CONSUME-OUT

                IF KAFKA-MESSAGE-REF OF KAFKA-CONSUME-OUT
                   NOT = NULL AND ERROR-CODE = 0

                   MOVE MSG-LENGTH TO KAFKA-PAYLOAD-LEN
                   MOVE MSG-PARTITION TO PAYLOAD-PARTITION
                   MOVE MSG-OFFSET TO PAYLOAD-OFFSET

                   IF CALLER-31BIT OF CONSUMER-INPUT = 'Y'

                      SET KAFKA-PTR TO MSG-PAYLOAD

                      CALL CONV-PGM USING CONV-INPUT RETURNING
                         CONV-OUTPUT

                      IF CONV-MSG-RESPONSE OF CONV-OUTPUT NOT = 0
                         MOVE CONV-MSG TO KAFKA-MSG
                         MOVE CONV-MSG-RESPONSE TO KAFKA-MSG-RESPONSE
                         MOVE 16 TO RETURN-CODE
                         GOBACK
                      END-IF

                      SET KAFKA-PAYLOAD-31 OF CONSUMER-OUTPUT
                         TO KAFKA-PAYLOAD-31 OF CONV-OUTPUT

                   ELSE
                      SET KAFKA-PAYLOAD-64 OF CONSUMER-OUTPUT TO
                         MSG-PAYLOAD
                   END-IF

                ELSE
                   IF KAFKA-MESSAGE-REF OF KAFKA-CONSUME-OUT
                      = NULL

                      DISPLAY "WITHIN LAST ERROR "
                      MOVE FUNCTION IXY-KAFKA-LAST-ERROR
                         TO RETURN-STATUS OF KAFKA-LAST-ERROR-OUT

                      IF RETURN-STATUS OF KAFKA-LAST-ERROR-OUT = 0
                         MOVE "FAILURE WHILE GETTING LAST ERROR"
                            TO KAFKA-MSG
                         MOVE 9009 TO KAFKA-MSG-RESPONSE
                         GOBACK
                      END-IF

                      IF RETURN-STATUS OF KAFKA-LAST-ERROR-OUT = -191
                         MOVE RETURN-STATUS OF KAFKA-LAST-ERROR-OUT
                            TO RETURN-STATUS OF KAFKA-ERR2STR-IN
                         PERFORM GENERATE-ERR-STR

                      END-IF

                   ELSE
                      IF ERROR-CODE = -191
                         DISPLAY "WITHIN CONDITION"

                         MOVE ERROR-CODE TO
                              RETURN-STATUS OF KAFKA-ERR2STR-IN
                         PERFORM GENERATE-ERR-STR
                      ELSE
                         MOVE ERROR-CODE TO
                              RETURN-STATUS OF KAFKA-ERR2STR-IN
                         PERFORM GENERATE-ERR-STR
                         GOBACK
                      END-IF
                   END-IF
                END-IF

                SET KAFKA-MESSAGE-REF OF KAFKA-MESSAGE-DESTROY-IN
                   TO KAFKA-MESSAGE-REF OF KAFKA-CONSUME-OUT

                MOVE FUNCTION IXY-KAFKA-MESSAGE-DESTROY
                   (
                   KAFKA-MESSAGE-REF OF KAFKA-MESSAGE-DESTROY-IN
                   ) TO
                   RETURN-STATUS OF KAFKA-MESSAGE-DESTROY-OUT

                IF RETURN-STATUS OF KAFKA-MESSAGE-DESTROY-OUT NOT = 0
                   MOVE 9013 TO KAFKA-MSG-RESPONSE
                   MOVE 'KAFKA-MESSAGE-DESTROY FAILED' TO KAFKA-MSG
                   GOBACK
                END-IF

           WHEN KAFKA-DELETE
      ******************************************************************
      * The KAFKA-DELETE block handles the cleanup and deletion of Kafka
      * consumer resources. It begins by iterating through all consumed
      * partitions and stops consumption for each partition using the
      * IXY-KAFKA-CONSUME-STOP function. After stopping all partitions, 
      * the program proceeds to destroy the Kafka topic using 
      * IXY-KAFKA-TOPIC-DESTROY. Finally, the Kafka instance itself is 
      * destroyed using IXY-KAFKA-DESTROY.
      ******************************************************************
                MOVE 1 TO WS-RCNT

                PERFORM UNTIL WS-RCNT > TOTAL-PARTNOS
                        SET KAFKA-TOPIC-REF OF KAFKA-CONSUME-STOP-IN TO
                           KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT
                        MOVE RESTART-PARTITION(WS-RCNT) TO PARTITION OF
                           KAFKA-CONSUME-STOP-IN
                        ADD 1 TO WS-RCNT

                        MOVE FUNCTION IXY-KAFKA-CONSUME-STOP
                           (
                           KAFKA-TOPIC-REF OF KAFKA-CONSUME-STOP-IN
                           PARTITION OF KAFKA-CONSUME-STOP-IN
                           )
                           TO RETURN-STATUS OF KAFKA-CONSUME-STOP-OUT

                        IF RETURN-STATUS OF KAFKA-CONSUME-STOP-OUT
                           NOT = 0
                           MOVE RETURN-STATUS OF KAFKA-LAST-ERROR-OUT TO
                              RETURN-STATUS OF KAFKA-ERR2STR-IN

                           PERFORM GENERATE-ERR-STR
                           GOBACK
                        END-IF
                END-PERFORM

                SET KAFKA-TOPIC-REF OF KAFKA-TOPIC-DESTROY-IN TO
                   KAFKA-TOPIC-REF OF KAFKA-TOPIC-NEW-OUT

                MOVE FUNCTION IXY-KAFKA-TOPIC-DESTROY(
                   KAFKA-TOPIC-REF OF KAFKA-TOPIC-DESTROY-IN
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
                   KAFKA-TYPE-REF OF KAFKA-NEW-OUT

                MOVE FUNCTION IXY-KAFKA-DESTROY(
                   KAFKA-TYPE-REF OF KAFKA-DESTROY-IN
                   ) TO
                   RETURN-STATUS OF KAFKA-DESTROY-OUT

                IF RETURN-STATUS OF KAFKA-DESTROY-OUT NOT = 0
                   MOVE "FAILURE IN KAFKA-DESTROY" TO KAFKA-MSG
                   MOVE 9006 TO KAFKA-MSG-RESPONSE
                   GOBACK
                END-IF
                MOVE 0 TO KAFKA-MSG-RESPONSE
                MOVE "KAFKA CONSUMER DESTROY DONE" TO KAFKA-MSG

           WHEN OTHER
      ******************************************************************
      * If the Kafka action is invalid or unrecognized, the program sets
      * an error message and exits with code 9999.
      ******************************************************************
                MOVE "INVALID KAFKA-ACTION FOR CONSUMER"
                   TO KAFKA-MSG
                MOVE 9999 TO KAFKA-MSG-RESPONSE
                GOBACK
           END-EVALUATE

           GOBACK
           .

       GET-LAST-ERR.
      ******************************************************************
      * The GET-LAST-ERR paragraph retrieves the most recent Kafka error
      * code using the IXY-KAFKA-LAST-ERROR function.
      ******************************************************************
           MOVE FUNCTION IXY-KAFKA-LAST-ERROR
              TO RETURN-STATUS OF KAFKA-LAST-ERROR-OUT

           IF RETURN-STATUS OF KAFKA-LAST-ERROR-OUT = 0
              MOVE "FAILURE WHILE GETTING LAST ERROR"
                 TO KAFKA-MSG
              MOVE 9009 TO KAFKA-MSG-RESPONSE
              GOBACK
           END-IF
           .

       CONVERT-EBC-ASC.
      ******************************************************************
      * The CONVERT-EBC-ASC paragraph performs the conversion of
      * EBCDIC-encoded data to ASCII format using the EBCDIC-ASCII-CONV
      * function.
      ******************************************************************
           MOVE FUNCTION EBCDIC-ASCII-CONV(
              EBCDIC-DATA-PTR OF EBCDIC-ASCII-CONV-IN
              ) TO
              ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT.

           IF ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT = 0
              MOVE "FAILURE WHILE CONVERTING EBCDIC DATA TO ASCII"
                 TO KAFKA-MSG
              MOVE 9007 TO KAFKA-MSG-RESPONSE
              GOBACK
           END-IF
           .

       CONVERT-ASC-EBC.
      ******************************************************************
      * The CONVERT-ASC-EBC paragraph handles the conversion of
      * ASCII-encoded data to EBCDIC format using the ASCII-EBCDIC-CONV
      * function.
      ******************************************************************
           MOVE FUNCTION ASCII-EBCDIC-CONV(
              ASCII-DATA-PTR OF ASCII-EBCDIC-CONV-IN
              ) TO
              EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT

           IF EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT = 0
              MOVE "FAILURE WHILE CONVERTING ASCII DATA TO EBCDIC"
                 TO KAFKA-MSG
              MOVE 9008 TO KAFKA-MSG-RESPONSE
              GOBACK
           END-IF
           .

       GENERATE-ERR-STR.
      ******************************************************************
      * The GENERATE-ERR-STR paragraph is responsible for converting a
      * Kafka error code into a human-readable error message string.
      ******************************************************************
           MOVE FUNCTION IXY-KAFKA-ERR2STR(
              RETURN-STATUS OF KAFKA-ERR2STR-IN
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
              KAFKA-MSG-RESPONSE
           .

       END PROGRAM 'IXYSCONS'.