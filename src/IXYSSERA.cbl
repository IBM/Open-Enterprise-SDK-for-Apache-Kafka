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
      * FUNCTION PROTOTYPES
      ******************************************************************
       COPY IXYSPTYP.
       COPY IXYPROTP.
      ******************************************************************
      * MAIN MODULE SERIALIZATION AVRO
      ******************************************************************
      * This sample module does the below when invoked with specific
      * Type
      * a. Initialization of Serdes objects
      * b. converts messages from Json to Avro format and Serialization 
      *    of AVRO message
      * d. Destroy the Serdes object
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYSSERA'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.

      ******************************************************************
      * INPUT-OUTPUT DATA COPYBOOK
      ******************************************************************
         COPY IXYSCOPY.
         COPY IXYCOPY.

         01 CONV-INPUT.
            COPY IXYCONVI.
         01 CONV-OUTPUT.
            COPY IXYCONVO.
         01 CONV-PGM                PIC X(8) VALUE "IXYSCONV".

       >>DATA 31
         01 HOST-TEMP               PIC X(1024).
         01 VALUE-TEMP              PIC X(1024).
         01 SCHEMA-TEMP             PIC X(1024).
         01 ERR-STG                 PIC X(1024) VALUE SPACES.
         01 ERR-LEN                 PIC 9(18) BINARY VALUE 1024.
         01 WS-CNT                  PIC 9(2)  BINARY VALUE 1.
         01 END-OF-STRING           PIC X(01) VALUE X'00'.
         01 INDEX-POS               PIC 9(04) BINARY VALUE 0.

       LINKAGE SECTION.
         01 SERIAL-AVRO-INPUT.
            COPY IXYSERAI.
         01 SERIAL-AVRO-OUTPUT.
            COPY IXYSERAO.
         01 DATA-TEMP               PIC X(1024).

       PROCEDURE DIVISION USING SERIAL-AVRO-INPUT
           RETURNING SERIAL-AVRO-OUTPUT.

           EVALUATE TRUE
      * SERDES Initialization
           WHEN SERDES-INIT
      * Create SERDES CONF pointer
            INITIALIZE ERR-STG
            MOVE 0   TO ERRSTR-SIZE OF SERDES-CONF-NEW-IN
            SET ERROR-PTR OF SERDES-CONF-NEW-IN TO
                            NULL

            SET SERDES-CONF-REF OF SERDES-CONF-NEW-OUT
                            TO
              FUNCTION IXY-SERDES-CONF-NEW(
                  ERROR-PTR   OF SERDES-CONF-NEW-IN
                  ERRSTR-SIZE OF SERDES-CONF-NEW-IN)

            IF SERDES-CONF-REF OF SERDES-CONF-NEW-OUT = NULL
              DISPLAY "**ERROR** : FAILURE FROM SERDES-CONF-NEW"
              MOVE 9001 TO SERDES-MSG-RESPONSE
              PERFORM GENERATE-ERROR-STR-ASC-EBC
              GOBACK
            END-IF

      * SERDES SET Configuration
            SET SERDES-CONF-REF OF SERDES-CONF-SET-IN
                  TO SERDES-CONF-REF OF SERDES-CONF-NEW-OUT

      * Set all the Configuration properties
            PERFORM UNTIL WS-CNT > NUM-OF-PARMS

      * Convert Config name to ASCII
              SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                            TO ADDRESS OF CONFIG-NAME(WS-CNT)
              PERFORM CONVERT-EBC-ASC
              SET ADDRESS OF DATA-TEMP
                  TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
              MOVE DATA-TEMP TO HOST-TEMP
              SET PARM-NAME-PTR OF SERDES-CONF-SET-IN
                            TO ADDRESS OF HOST-TEMP

      * Convert Config value to ASCII
              SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                            TO ADDRESS OF CONFIG-VALUE(WS-CNT)
              PERFORM CONVERT-EBC-ASC
              SET ADDRESS OF DATA-TEMP
                  TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
              MOVE DATA-TEMP TO VALUE-TEMP
              SET PARM-VALUE-PTR OF SERDES-CONF-SET-IN
                            TO ADDRESS OF VALUE-TEMP

              INITIALIZE ERR-STG
              MOVE ERR-LEN   TO ERRSTR-SIZE OF SERDES-CONF-SET-IN
              SET ERRSTR-PTR OF SERDES-CONF-SET-IN TO
                            ADDRESS OF ERR-STG

              MOVE FUNCTION IXY-SERDES-CONF-SET(
                            SERDES-CONF-REF OF SERDES-CONF-SET-IN
                            PARM-NAME-PTR   OF SERDES-CONF-SET-IN
                            PARM-VALUE-PTR  OF SERDES-CONF-SET-IN
                            ERRSTR-PTR      OF SERDES-CONF-SET-IN
                            ERRSTR-SIZE     OF SERDES-CONF-SET-IN
                            )
                            TO SERDES-CONF-RES OF SERDES-CONF-SET-OUT

              IF SERDES-CONF-RES NOT = 0
                DISPLAY "**ERROR** : FAILURE FROM SERDES-CONF-SET"
                MOVE CONF-RES TO SERDES-MSG-RESPONSE
                PERFORM GENERATE-ERROR-STR-ASC-EBC
                GOBACK
              END-IF

              ADD 1 TO WS-CNT
            END-PERFORM

      * Create SERDES Object

            SET  SERDES-CONF-REF OF SERDES-NEW-IN
                            TO SERDES-CONF-REF OF SERDES-CONF-SET-IN

            INITIALIZE ERR-STG
            MOVE ERR-LEN   TO ERRSTR-SIZE OF SERDES-NEW-IN
            SET ERRSTR-PTR OF SERDES-NEW-IN TO
                            ADDRESS OF ERR-STG

            SET SERDES-T-REF OF SERDES-NEW-OUT TO
                  FUNCTION IXY-SERDES-NEW(
                            SERDES-CONF-REF OF SERDES-NEW-IN
                            ERRSTR-PTR     OF SERDES-NEW-IN
                            ERRSTR-SIZE    OF SERDES-NEW-IN
                            )

            IF SERDES-T-REF OF SERDES-NEW-OUT = NULL
               DISPLAY "**ERROR** : FAILURE FROM SERDES-NEW"
               MOVE 9002 TO SERDES-MSG-RESPONSE
               PERFORM GENERATE-ERROR-STR-ASC-EBC
               GOBACK
             ELSE
               MOVE 0 TO SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT
               MOVE "SERDES INIT SUCCESS" TO SERDES-MSG
            END-IF

      * Serialization of Json Message
           WHEN SERDES-SER

             SET SERDES-T-REF OF SERDES-SERIALIZE-IN TO
               SERDES-T-REF OF SERDES-NEW-OUT

             IF CALLER-31BIT = 'Y'
               SET JSON-MSG-PTR OF SERDES-SERIALIZE-IN TO
                                              JSON-PAYLOAD-31
             ELSE
               SET JSON-MSG-PTR OF SERDES-SERIALIZE-IN TO
                                              JSON-PAYLOAD-64
             END-IF

             SET EBCDIC-DATA-PTR-31 OF EBCDIC-ASCII-CONV-IN
                TO ADDRESS OF SCHEMA-NAME OF SERIAL-AVRO-INPUT

             PERFORM CONVERT-EBC-ASC

             SET ADDRESS OF DATA-TEMP
                  TO ASCII-DATA-PTR-31 OF EBCDIC-ASCII-CONV-OUT
             MOVE DATA-TEMP TO SCHEMA-TEMP
             SET SCHEMA-NAME-PTR OF SERDES-SERIALIZE-IN TO  
                  ADDRESS OF SCHEMA-TEMP

             SET ERRSTR-PTR OF SERDES-SERIALIZE-IN TO
                            ADDRESS OF ERR-STG

             MOVE FUNCTION IXY-SERDES-SERIALIZE(
                        SERDES-T-REF OF SERDES-SERIALIZE-IN
                        SCHEMA-NAME-PTR OF SERDES-SERIALIZE-IN
                        JSON-MSG-PTR OF SERDES-SERIALIZE-IN
                        PAYLOAD-PTR OF SERDES-SERIALIZE-IN
                        PAYLOAD-SIZE OF SERDES-SERIALIZE-IN
                        ERRSTR-PTR OF SERDES-SERIALIZE-IN) TO
                       RETURN-STATUS OF SERDES-SERIALIZE-OUT

             IF RETURN-STATUS OF SERDES-SERIALIZE-OUT
               NOT = 0
                 DISPLAY "ERROR WHILE SERIALIZE"
                 MOVE ERR-STG TO SERDES-MSG
                 MOVE RETURN-STATUS OF SERDES-SERIALIZE-OUT TO
                   SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT
                 GOBACK
             ELSE
               IF CALLER-31BIT = 'Y'
                 SET KAFKA-PTR TO PAYLOAD-PTR OF SERDES-SERIALIZE-IN

                 CALL CONV-PGM USING CONV-INPUT RETURNING
                                     CONV-OUTPUT

                 IF CONV-MSG-RESPONSE OF CONV-OUTPUT NOT = 0
                   MOVE CONV-MSG TO SERDES-MSG
                   MOVE CONV-MSG-RESPONSE TO SERDES-MSG-RESPONSE
                   MOVE 16 TO RETURN-CODE
                   GOBACK
                 END-IF

                 SET SER-PAYLOAD-31 OF SERIAL-AVRO-OUTPUT
                     TO KAFKA-PAYLOAD-31 OF CONV-OUTPUT

               ELSE
                 SET  SER-PAYLOAD-64 OF SERIAL-AVRO-OUTPUT TO
                      PAYLOAD-PTR OF SERDES-SERIALIZE-IN
               END-IF

               MOVE PAYLOAD-SIZE OF SERDES-SERIALIZE-IN TO
                  SER-PAYLOAD-SIZE OF SERIAL-AVRO-OUTPUT

               MOVE 0 TO SERDES-MSG-RESPONSE
               MOVE "SERDES SERIALIZATION DONE" TO SERDES-MSG
             END-IF

      * Clean-up
           WHEN SERDES-DESTROY

             SET SERDES-T-REF OF SERDES-DESTROY-IN TO
               SERDES-T-REF OF SERDES-NEW-OUT

             MOVE FUNCTION IXY-SERDES-DESTROY(
              SERDES-T-REF OF SERDES-DESTROY-IN) TO
                   SERDES-DESTROY-RES OF SERDES-DESTROY-OUT

             IF SERDES-DESTROY-RES OF SERDES-DESTROY-OUT NOT = 0
               MOVE "FAILURE IN SERDES-DESTROY" TO SERDES-MSG
               MOVE SERDES-DESTROY-RES OF SERDES-DESTROY-OUT TO
                                               SERDES-MSG-RESPONSE
               GOBACK
             ELSE
               MOVE 0 TO SERDES-MSG-RESPONSE
               MOVE "SERDES DESTROY DONE" TO SERDES-MSG
             END-IF

           WHEN OTHER

             MOVE "INVALID SERDES-ACTION FOR SERIALIZATION"
               TO SERDES-MSG
             MOVE 9999 TO SERDES-MSG-RESPONSE
             GOBACK
           END-EVALUATE.

           GOBACK.

      * Convert EBCDIC to ASCII
       CONVERT-EBC-ASC.
           MOVE FUNCTION EBCDIC-ASCII-CONV(
                   EBCDIC-DATA-PTR OF EBCDIC-ASCII-CONV-IN
                   ) TO
                   ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT.

           IF ASCII-DATA-PTR OF EBCDIC-ASCII-CONV-OUT = 0
             MOVE "FAILURE WHILE CONVERTING EBSIDIC DATA TO ASCII"
                                                 TO SERDES-MSG
             MOVE 9007 TO SERDES-MSG-RESPONSE
             GOBACK
           END-IF.

      * Generate Error String in EBCDIC
       GENERATE-ERROR-STR-ASC-EBC.

           SET ASCII-DATA-PTR-31 OF ASCII-EBCDIC-CONV-IN
                 TO ADDRESS OF ERR-STG

           MOVE FUNCTION ASCII-EBCDIC-CONV(
                 ASCII-DATA-PTR OF ASCII-EBCDIC-CONV-IN
                 ) TO
                 EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT

           IF EBCDIC-DATA-PTR OF ASCII-EBCDIC-CONV-OUT = 0
             MOVE "FAILURE WHILE CONVERTING ASCII DATA TO EBSIDIC"
                                                 TO SERDES-MSG
             MOVE 9008 TO SERDES-MSG-RESPONSE
             GOBACK
           END-IF

           SET ADDRESS OF DATA-TEMP
                 TO EBCDIC-DATA-PTR-31 OF ASCII-EBCDIC-CONV-OUT
           MOVE 0 TO INDEX-POS

           INSPECT DATA-TEMP TALLYING INDEX-POS FOR CHARACTERS
                 BEFORE INITIAL END-OF-STRING

           MOVE DATA-TEMP(1:INDEX-POS) TO SERDES-MSG.


       END PROGRAM 'IXYSSERA'.