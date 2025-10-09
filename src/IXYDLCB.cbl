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
      * FUNCTION NAME         : DELIVERY-CALLBACK
      * EXTERNALIZED NAME     : IXYDLCB
      ******************************************************************
      * This is a sample Delivery report callback function. The pointer
      * to this function is passed as KAFKA-CALLBACK-REF in function -
      * IXY-KAFKA-DELIVERY-MSG-CB
      ******************************************************************
       IDENTIFICATION DIVISION.
         FUNCTION-ID. DELIVERY-CALLBACK AS "IXYDLCB"
           ENTRY-INTERFACE IS DYNAMIC
           ENTRY-NAME IS COMPAT.
       DATA DIVISION.
        WORKING-STORAGE SECTION.
         01 INDEX-POS             PIC 9(04) BINARY.
         01 MSG-SIZE-D            PIC z,zzz,zz9.
         01 MSG-PART-D            PIC z,zzz,zz9.
         01 MSG-OFFSET-D          PIC z,zzz,zz9.
        LINKAGE SECTION.
         01 RD-KAFKA-T            USAGE POINTER.
         01 RD-KAFKA-MESSAGE-T-STRUCT.
            05 ERROR-CODE         PIC S9(9) BINARY.
            05 FILLER             PIC X(04).
            05 MSG-TOPIC          USAGE POINTER.
            05 MSG-PART           PIC S9(9) BINARY.
            05 FILLER             PIC X(04).
            05 MSG-REF            USAGE POINTER.
            05 MSG-SIZE           PIC S9(18) BINARY.
            05 MSG-KEY-REF        USAGE POINTER.
            05 MSG-KEY-SIZE       PIC S9(18) BINARY.
            05 MSG-OFFSET         PIC S9(18) BINARY.
            05 MSG-PRIVATE        USAGE POINTER.
         01 OPAQUE                USAGE POINTER.
         01 RETURN-STATUS         PIC S9(18) BINARY.
       PROCEDURE DIVISION USING RD-KAFKA-T RD-KAFKA-MESSAGE-T-STRUCT
                          OPAQUE RETURNING RETURN-STATUS.

           IF ERROR-CODE = 0 THEN
              MOVE MSG-SIZE      TO MSG-SIZE-D
              MOVE MSG-PART      TO MSG-PART-D
              MOVE MSG-OFFSET    TO MSG-OFFSET-D
              DISPLAY "**********************************************"
              DISPLAY "MESSAGE DELIVERED SUCCESSFULLY "
              DISPLAY "  MESSAGE SIZE    : " MSG-SIZE-D " BYTES "
              DISPLAY "  PARTITION       : " MSG-PART-D
              DISPLAY "  OFFSET          : " MSG-OFFSET-D
              DISPLAY "**********************************************"
           ELSE
              DISPLAY "**********************************************"
              DISPLAY "MESSAGE DELIVERY FAILED "
              DISPLAY "  ERROR-CODE   : " ERROR-CODE
              DISPLAY "**********************************************"
           END-IF

           MOVE ERROR-CODE TO RETURN-STATUS
           GOBACK.
       END FUNCTION DELIVERY-CALLBACK.
