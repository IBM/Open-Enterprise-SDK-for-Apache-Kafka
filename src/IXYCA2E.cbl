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
      * FUNCTION NAME     : ASCII-EBCDIC-CONV
      * EXTERNALIZED NAME : IXYCA2E
      ******************************************************************
      * This function converts ASCII string to EBCDIC string upto
      * 1024 bytes. CCSID used for ASCII is 819 and EBCDIC is 1047
      * Parms:
      *    ASCII-DATA-PTR  - Pointer to ASCII String
      * Response:
      *    EBCDIC-DATA-PTR - Pointer to EBCDIC String
      ******************************************************************
      * Note: Update the values of the variables WS-CCSID-ASC and
      * WS-CCSID-EBC with the CCSIDs of ASCII and EBCDIC based on the
      * environment.
      ******************************************************************
       IDENTIFICATION DIVISION.
         FUNCTION-ID. ASCII-EBCDIC-CONV AS "IXYCA2E"
           ENTRY-INTERFACE IS DYNAMIC
           ENTRY-NAME IS COMPAT.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
         01 WS-CCSID-ASC          PIC 9(5) VALUE 819.
         01 WS-CCSID-EBC          PIC 9(5) VALUE 1047.
         01 NATIONAL-DATA         PIC N(1024).
       >>DATA 31
         01 EBCDIC-DATA-31        PIC X(1024).
       >>DATA 64
        LINKAGE SECTION.
         01 ASCII-DATA            PIC X(1024).
         01 ASCII-DATA-PTR        PIC 9(9) USAGE COMP-5.
         01 ASCII-DATA-PTR-31     REDEFINES
                                  ASCII-DATA-PTR USAGE POINTER-32.
         01 EBCDIC-DATA-PTR       PIC 9(9) USAGE COMP-5.
         01 EBCDIC-DATA-PTR-31    REDEFINES
                                  EBCDIC-DATA-PTR USAGE POINTER-32.
       PROCEDURE DIVISION USING ASCII-DATA-PTR
                      RETURNING EBCDIC-DATA-PTR.
           SET ADDRESS OF ASCII-DATA
                                  TO ASCII-DATA-PTR-31
           MOVE FUNCTION NATIONAL-OF(ASCII-DATA, WS-CCSID-ASC)
                                  TO NATIONAL-DATA
           MOVE FUNCTION DISPLAY-OF(NATIONAL-DATA, WS-CCSID-EBC)
                                  TO EBCDIC-DATA-31
           SET EBCDIC-DATA-PTR-31 TO ADDRESS OF EBCDIC-DATA-31
           GOBACK.
       END FUNCTION ASCII-EBCDIC-CONV.
