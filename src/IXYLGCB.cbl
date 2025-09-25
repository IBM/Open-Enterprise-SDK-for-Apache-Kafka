       CBL RENT EXPORTALL
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
      * FUNCTION NAME         : LOG-CALLBACK
      * EXTERNALIZED NAME     : IXYLGCB
      ******************************************************************
      * This is a Log callback function. The pointer to this function is
      * passed as CALLBACK-REF in function - IXY-KAFKA-CONF-SET-LOG-CB.
      * This Module converts the message passed as input from ASCII to 
      * EBCIDIC format and then displays it in JES Log.
      * This function converts ASCII string to EBCDIC string upto
      * 2048 bytes. CCSID used for ASCII is 819 and EBCDIC is 1047
      ******************************************************************
      * Note: Update the values of the variables WS-CCSID-ASC and
      * WS-CCSID-EBC with the CCSIDs of ASCII and EBCDIC based on the
      * environment.
      ******************************************************************      
       IDENTIFICATION DIVISION.
         FUNCTION-ID. LOG-CALLBACK AS "IXYLGCB"
           ENTRY-INTERFACE IS DYNAMIC
           ENTRY-NAME IS COMPAT.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
         01 WS-CCSID-ASC          PIC 9(5) VALUE 819.
         01 WS-CCSID-EBC          PIC 9(5) VALUE 1047.
         01 NATIONAL-DATA         PIC N(2048).
         01 EBCDIC-DATA           PIC X(2048).
         01 WS-COUNT              PIC 9(4) VALUE 0.
         01 ERROR-STRING          PIC X(2048).
         01 WS-FIRST-FLAG         PIC X(1) VALUE 'Y'.
       LINKAGE SECTION.
         01 RD-KAFKA-T            USAGE POINTER.
         01 LEVEL                 PIC S9(4) COMP-5 SYNC.
         01 FAC                   PIC X(10).
         01 BUF                   PIC X(2048).
         01 RETURN-STATUS         PIC S9(18) BINARY.
       PROCEDURE DIVISION USING RD-KAFKA-T LEVEL FAC BUF
                                  RETURNING RETURN-STATUS.

           MOVE FUNCTION NATIONAL-OF(BUF, WS-CCSID-ASC)
                                  TO NATIONAL-DATA
           MOVE FUNCTION DISPLAY-OF(NATIONAL-DATA, WS-CCSID-EBC)
                                  TO EBCDIC-DATA
           MOVE 0 TO WS-COUNT
           INSPECT EBCDIC-DATA TALLYING WS-COUNT FOR
                   CHARACTERS BEFORE INITIAL X'00'
           IF WS-COUNT > 0
             MOVE EBCDIC-DATA(1:WS-COUNT) TO ERROR-STRING
           ELSE
             MOVE EBCDIC-DATA TO ERROR-STRING
           END-IF
           DISPLAY "Log : " ERROR-STRING(1:WS-COUNT)
           MOVE 0 TO RETURN-STATUS
           GOBACK.
       END FUNCTION LOG-CALLBACK.