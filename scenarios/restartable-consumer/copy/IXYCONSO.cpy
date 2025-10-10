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
      **               KAFKA CONSUMER OUT COPYBOOK                    **
      ******************************************************************
      ** This copybook is used in consumer program to return the      **
      ** output parameters to the calling program. Sample usage is of **
      ** this provided in IXYCON31,IXYCON64,IXYCNS31 and IXYCNS64     **
      ** programs calling IXYSCONS program.                           **
      ******************************************************************
            05 KAFKA-MESSAGE-31.
               10 KAFKA-PAYLOAD-31-PTR PIC 9(9) COMP-5.
               10 KAFKA-PAYLOAD-31  REDEFINES KAFKA-PAYLOAD-31-PTR
                                        USAGE POINTER-32.
            05 KAFKA-MESSAGE-64.
               10 KAFKA-PAYLOAD-64-PTR PIC 9(18) COMP-5.
               10 KAFKA-PAYLOAD-64     REDEFINES KAFKA-PAYLOAD-64-PTR
                                       USAGE POINTER.
            05 KAFKA-PAYLOAD-LEN       PIC S9(18) BINARY.
            05 KAFKA-MSG               PIC X(1024).
            05 KAFKA-MSG-RESPONSE      PIC S9(9) BINARY.
            05 PAYLOAD-PARTITION       PIC S9(9) BINARY.
            05 PAYLOAD-OFFSET          PIC S9(18) BINARY.