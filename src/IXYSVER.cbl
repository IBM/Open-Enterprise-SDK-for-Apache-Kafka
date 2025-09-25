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
      * MAIN PROGRAM KAFKA VERSION
      ******************************************************************
      * This sample module gets the KAFKA Version.
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYSVER'.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
      ******************************************************************
      * KAFKA INPUT-OUTPUT DATA COPYBOOK
      ******************************************************************
         COPY IXYCOPY.
      ******************************************************************
      *  REQUIRED VARIABLES
      ******************************************************************
       >>DATA 31
         01 END-OF-STRING           PIC X(01) VALUE X'00'.
         01 INDEX-POS               PIC 9(04) BINARY.

       LINKAGE SECTION.
         01 KAFKA-VERSION           PIC X(1024).

       PROCEDURE DIVISION.

      * CALL IXY-KAFKA-VER to find Kafka version
           MOVE FUNCTION IXY-KAFKA-VER
                TO KAFKA-VER-PTR OF KAFKA-VERSION-STR-OUT
           SET ADDRESS OF KAFKA-VERSION
                TO KAFKA-VER-PTR-31 OF KAFKA-VERSION-STR-OUT
           INSPECT KAFKA-VERSION TALLYING INDEX-POS FOR CHARACTERS
                   BEFORE INITIAL END-OF-STRING
           DISPLAY "KAFKA VERSION: "
                   KAFKA-VERSION(1:INDEX-POS)
           GOBACK.
       END PROGRAM 'IXYSVER'.