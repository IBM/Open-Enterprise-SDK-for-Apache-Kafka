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
      * MAIN PROGRAM IXYSCONV
      ******************************************************************
      * This Module converts the 64 bit Payload Pointer to 31 bit
      * message and then passes the 31 bit Pointer back to the 31 bit
      * calling module. This module is called by Consumer Main Module
      * IXYSCONS, if caller program or module is 31 bit application.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYSCONV'.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       >>DATA 31
         01 KAFKA-MSG-ASCII-31      PIC X(1024).

        LINKAGE SECTION.
         01 CONV-INPUT.
            COPY IXYCONVI.
         01 CONV-OUTPUT.
            COPY IXYCONVO.
         01 KAFKA-MSG-ASCII-64      PIC X(1024).

        PROCEDURE DIVISION USING CONV-INPUT
                   RETURNING CONV-OUTPUT.

           DISPLAY "KAFKA AMODE 64 BIT CONV PROGRAM"

           INITIALIZE CONV-OUTPUT

           SET ADDRESS OF KAFKA-MSG-ASCII-64 TO KAFKA-PTR

           MOVE KAFKA-MSG-ASCII-64  TO KAFKA-MSG-ASCII-31

           SET KAFKA-PAYLOAD-31
                   TO ADDRESS OF KAFKA-MSG-ASCII-31
             .
       END PROGRAM 'IXYSCONV'.
