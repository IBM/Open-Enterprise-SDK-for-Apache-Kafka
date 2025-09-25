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
      * MAIN PROGRAM IXYCAV64
      ******************************************************************
      * This is a sample program compiled in 64 bit addressing mode.
      * This Program is used to Consume Avro Mesaage from Kafka,
      * De-Serialize it and then convert the message to COBOL Copybook
      * format.
      *
      * This program uses
      * a. CCONFFIL file to get the Configuration parameters needed.
      * b. TOPICFIL file to get the topic details.
      * c. SCONFFIL file to get the Serdes configuration details.
      * d. Initialize Kafka and Serdes Configurations using IXYSCONS
      *    and IXYSDSEA module with Init option.
      * e. Calls IXYSCONS module and consume the message from kafka
      *    using Consume Option.
      * f. Once message is consumed, Avro Message consumed is
      *    De-Serialized and converted to JSON message using IXYSDSEA.
      * g. Json Message is converted to COBOL structure using the data
      *    transformation utility generated code IXYCNJSN and IXYJS2CP.
      * h. Display the COBOL copybook formatted data
      * i. Destroy the Kafka and Serdes objects using IXYSCONS and
      *    IXYSDSEA with Destroy option.
      *
      * The program should be modified with the following changes:
      * 1) The value of PART-VAL should be set to the target partition
      *    value.
      * 2) The value of PART-LIST-SIZE should be set to the size of
      *    topic partition list.
      * 3) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 4) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events
      * 5) TOPICFIL - This is the file which contains the topic details.
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the topic length crosses 2049 bytes.
      * 6) CCONFFIL - Kafka Consumer Configuration file
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the Configuration length crosses 2049 bytes
      * 7) SCONFFIL - Serdes Configuration file
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the Configuration length crosses 2049 bytes
      * 8) Change the Schema Name Accordingly and pass it to
      *    SCHEMA-NAME OF DESERIAL-AVRO-INPUT.
      * 9) Change the Data Transformation Utility generated snippets
      *     accordingly. i.e., IXYCNJSN, IXYCASC, IXYJS2CP and IXYVARSN
      ******************************************************************
       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYCAV64'.
       ENVIRONMENT DIVISION.
        INPUT-OUTPUT SECTION.
         FILE-CONTROL.

           SELECT CCONFFIL ASSIGN TO CCONFFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT TOPICFIL ASSIGN TO TOPICFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT SCONFFIL ASSIGN TO SCONFFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

       DATA DIVISION.
        FILE SECTION.

         FD CCONFFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  KAFKA-CCONFIG-FILE.

         01 KAFKA-CCONFIG-FILE.
            05 KAFKA-CCONFIG-REC      PIC X(2049).

         FD TOPICFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  TOPIC-DATA.

         01 TOPIC-DATA.
            05 TOPIC-DATA-REC     PIC X(2049).

         FD SCONFFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  SERDES-CONFIG-FILE.

         01 SERDES-CONFIG-FILE.
            05 SERDES-CONFIG-REC      PIC X(2049).

        WORKING-STORAGE SECTION.
      ******************************************************************
      *  CONSUMER Values
      ******************************************************************
         01 PART-VAL            PIC S9(9)  BINARY VALUE -1.
         01 PART-LIST-SIZE      PIC S9(09) BINARY VALUE 1.
         01 MSGFLGS-VAL         PIC X(01)  VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9)  BINARY VALUE 8000.
         01 TOPIC-LENGTH        PIC S9(4) BINARY VALUE 0000.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
         01 SER-MSG-ASC         PIC X(10000).
         01 WS-JSON-MSG         PIC X(10000).

      * File Status
         01 WS-FILE-STATUS      PIC 9(02).
         01 WS-EOF-SW           PIC X(01).
             88 WS-EOF          VALUE 'Y'.
             88 WS-NOT-EOF      VALUE 'N'.

      * Configuration file
         01 WS-CNT              PIC S9(9) BINARY VALUE 0000.
         01 WS-PARMLEN          PIC S9(9) BINARY VALUE 0000.
         01 WS-VALLEN           PIC S9(9) BINARY VALUE 0000.
         01 WS-SCHEMALEN        PIC S9(9) BINARY VALUE 0000.
         01 WS-DELIMITER-POS    PIC S9(9) BINARY VALUE 0000.

         01 KAFKA-CONFIG-DATA.
            05 KAFKA-CONFIG-PARM      PIC X(1024).
            05 WS-DELIMITER           PIC X VALUE '='.
            05 KAFKA-CONFIG-VALUE     PIC X(1024).

       >>DATA 31
      * Input/Output values for Consumer program
         01 CONSUMER-INPUT.
            COPY IXYCONSI.
         01 CONSUMER-OUTPUT.
            COPY IXYCONSO.
         01 CONSUMER-PGM        PIC X(8) VALUE "IXYSCONS".

      * Input/Output values for Serialization program
         01 DESERIAL-AVRO-INPUT.
            COPY IXYDSEAI.
         01 DESERIAL-AVRO-OUTPUT.
            COPY IXYDSEAO.
         01 DESERIAL-PGM        PIC X(8) VALUE "IXYSDSEA".

      ******************************************************************
      * COBOL DATA COPYBOOK GENERATED BY DATA TRANSFORMATION UTILITY
      ******************************************************************
         COPY IXYCASC.

         COPY IXYVARSN.

         01 CJ2C-PGM            PIC X(8) VALUE "IXYJS2CP".

       LINKAGE SECTION.
         01 JSON-MSG-ASCII       PIC X(10000).
         01 KAFKA-SER-MSG-ASCII  PIC X(10000).

       PROCEDURE DIVISION.
           DISPLAY "KAFKA AMODE 64 CONSUMER PROGRAM"

           PERFORM READ-CONSUMER-TOPIC
           PERFORM READ-CONSUMER-CONFIG
           PERFORM READ-SERDES-CONFIG
           PERFORM INIT-KAFKA-CONSUMER
           PERFORM INIT-SERDES-DSERIAL
           PERFORM KAFKA-CONSUME-MESSAGE
           PERFORM KAFKA-DSERIAL-MESSAGE
           PERFORM CONVERT-JSON-COPY
           PERFORM DISPLAY-COBOL-CONSUMED-DATA
           PERFORM DESTROY-KAFKA-CONSUME
           PERFORM DESTROY-SERDES-DSERIAL
           GOBACK
           .

       READ-CONSUMER-TOPIC.
      *****************************************************************
      * TOPICFIL is used to pass the Topic name to Kafka. Only
      * one topic name is being supported currently. Topic name should
      * be of maximum 2049 bytes. If its more than 2049 bytes, please
      * update the file description and use a different flat file
      * instead of standard file from the library.
      *****************************************************************

           OPEN INPUT TOPICFIL

           READ TOPICFIL

           INSPECT TOPIC-DATA-REC TALLYING TOPIC-LENGTH
                   FOR CHARACTERS BEFORE ' '

           MOVE FUNCTION TRIM(TOPIC-DATA-REC) TO
                      KAFKA-TOPIC-NAME OF
                        CONSUMER-INPUT(1:TOPIC-LENGTH)
           MOVE LOW-VALUE TO KAFKA-TOPIC-NAME OF
                        CONSUMER-INPUT(TOPIC-LENGTH + 1:)
           DISPLAY "Consumer Topic : " KAFKA-TOPIC-NAME OF
                              CONSUMER-INPUT(1:TOPIC-LENGTH)

           CLOSE TOPICFIL.

       READ-CONSUMER-CONFIG.
      *****************************************************************
      * CCONFFIL contains the Configuration Parameters which are needed
      * for setting up the KAFKA connection. Configuration file is read
      * and parsed to extract the configuration Parameter and its
      * value. Length of Configuration Parameter and its value is
      * determined. End of string (LOW VALUES) is appended to the
      * configuration parameter and value. This file can contain
      * comments starting with '#'. Parameter and Value is delimited
      * by '='.
      *****************************************************************

           SET WS-NOT-EOF TO TRUE
           MOVE 0 TO WS-CNT
           OPEN INPUT CCONFFIL
           PERFORM UNTIL WS-EOF
             READ CCONFFIL
             AT END SET WS-EOF TO TRUE
             NOT AT END
               IF KAFKA-CCONFIG-REC(1:1) NOT = '#'
                 MOVE 0 TO WS-DELIMITER-POS

                 INSPECT KAFKA-CCONFIG-REC TALLYING WS-DELIMITER-POS
                   FOR CHARACTERS BEFORE WS-DELIMITER

                 IF WS-DELIMITER-POS NOT = 0
                   MOVE KAFKA-CCONFIG-REC(1:WS-DELIMITER-POS) TO
                     KAFKA-CONFIG-PARM
                   MOVE KAFKA-CCONFIG-REC(WS-DELIMITER-POS + 2:) TO
                     KAFKA-CONFIG-VALUE
                 END-IF

                 ADD 1 TO NUM-OF-PARMS OF CONSUMER-INPUT
                 ADD 1 TO WS-CNT

                 COMPUTE WS-PARMLEN = FUNCTION LENGTH(
                   FUNCTION TRIM(KAFKA-CONFIG-PARM))
                 COMPUTE WS-VALLEN = FUNCTION LENGTH(
                   FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                 MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                      CONFIG-NAME OF
                      CONSUMER-INPUT(WS-CNT)(1:WS-PARMLEN)
                 MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                      CONFIG-VALUE OF
                      CONSUMER-INPUT(WS-CNT)(1:WS-VALLEN)

      * End of string identified using LOW VALUE in C. Hence appending
      * it to the end of each configuration and its parameters
                 MOVE LOW-VALUE TO CONFIG-NAME
                   OF CONSUMER-INPUT(WS-CNT)(WS-PARMLEN + 1:)
                 MOVE LOW-VALUE TO CONFIG-VALUE
                   OF CONSUMER-INPUT(WS-CNT)(WS-VALLEN + 1:)
               END-IF
             END-READ
           END-PERFORM

           CLOSE CCONFFIL.

       READ-SERDES-CONFIG.
      *****************************************************************
      * SCONFFIL contains the Configuration Parameters which are needed
      * for setting up the SERDES connection. Configuration file is read
      * and parsed to extract the configuration Parameter and its
      * value. Length of Configuration Parameter and its value is
      * determined. End of string (LOW VALUES) is appended to the
      * configuration parameter and value. This file can contain
      * comments starting with '#'. Parameter and Value is delimited
      * by '='.
      *****************************************************************
           OPEN INPUT SCONFFIL
           SET WS-NOT-EOF TO TRUE
           MOVE 0 TO WS-CNT
           PERFORM UNTIL WS-EOF
             READ SCONFFIL
             AT END SET WS-EOF TO TRUE
             NOT AT END
               IF SERDES-CONFIG-REC(1:1) NOT = '#'

                 MOVE 0 TO WS-DELIMITER-POS

                 INSPECT SERDES-CONFIG-REC TALLYING WS-DELIMITER-POS
                   FOR CHARACTERS BEFORE WS-DELIMITER

                 IF WS-DELIMITER-POS NOT = 0
                   MOVE SERDES-CONFIG-REC(1:WS-DELIMITER-POS) TO
                                       KAFKA-CONFIG-PARM
                   MOVE SERDES-CONFIG-REC(WS-DELIMITER-POS + 2:) TO
                                       KAFKA-CONFIG-VALUE
                 END-IF

                 ADD 1 TO NUM-OF-PARMS OF DESERIAL-AVRO-INPUT
                 ADD 1 TO WS-CNT

                 COMPUTE WS-PARMLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-PARM))
                 COMPUTE WS-VALLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                 MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                             CONFIG-NAME OF
                             DESERIAL-AVRO-INPUT(WS-CNT)(1:WS-PARMLEN)
                 MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                             CONFIG-VALUE OF
                              DESERIAL-AVRO-INPUT(WS-CNT)(1:WS-VALLEN)

      * End of string identified using LOW VALUE in C. Hence appending
      * it to the end of each configuration and its parameters
                 MOVE LOW-VALUE TO CONFIG-NAME
                    OF DESERIAL-AVRO-INPUT(WS-CNT)(WS-PARMLEN + 1:)
                 MOVE LOW-VALUE TO CONFIG-VALUE
                    OF DESERIAL-AVRO-INPUT(WS-CNT)(WS-VALLEN + 1:)
               END-IF
              END-READ
           END-PERFORM

           CLOSE SCONFFIL.

       INIT-KAFKA-CONSUMER.
      *****************************************************************
      * Invoke the Consumer program to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the Kafka config file
      *****************************************************************

      *    KAFKA-TYPE-PC is 0 for PRODUCER
      *    KAFKA-TYPE-PC is 1 for CONSUMER
           MOVE 1               TO KAFKA-TYPE-PC OF CONSUMER-INPUT
           MOVE PART-LIST-SIZE  TO KAFKA-PART-LIST-SIZE
             OF CONSUMER-INPUT
           MOVE PART-VAL        TO PARTITION-VALUE OF CONSUMER-INPUT
           MOVE MSGFLGS-VAL     TO MSGFLAGS-VALUE OF CONSUMER-INPUT
           MOVE TIMEOUT-MS      TO TIMEOUT-MS-VALUE OF CONSUMER-INPUT
           MOVE 'I'             TO KAFKA-ACTION OF CONSUMER-INPUT

           DISPLAY "KAFKA CONSUMER INIT BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
                             RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                            CONSUMER-OUTPUT)
             MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG OF CONSUMER-OUTPUT)
           END-IF.

       INIT-SERDES-DSERIAL.
      *****************************************************************
      * Invoke the De-Serialize module to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the Serdes config file
      *****************************************************************

           MOVE 'I'    TO SERDES-ACTION OF DESERIAL-AVRO-INPUT

           DISPLAY "KAFKA SERDES INIT BEGIN"

           CALL DESERIAL-PGM    USING DESERIAL-AVRO-INPUT
                   RETURNING DESERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                           DESERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             PERFORM DESTROY-KAFKA-CONSUME
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY FUNCTION TRIM(SERDES-MSG OF DESERIAL-AVRO-OUTPUT)
           END-IF.

       KAFKA-CONSUME-MESSAGE.
      *****************************************************************
      * The Events are retrieved from the kafka by invoking the
      * consumer prgram using Kafka Action as 'C'
      *****************************************************************
           MOVE 'C'  TO KAFKA-ACTION OF CONSUMER-INPUT
           DISPLAY "KAFKA CONSUME BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
                             RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
             MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                                  CONSUMER-OUTPUT)
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             PERFORM DESTROY-KAFKA-CONSUME
             PERFORM DESTROY-SERDES-DSERIAL
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             SET ADDRESS OF KAFKA-SER-MSG-ASCII TO 
                              KAFKA-PAYLOAD-64 OF CONSUMER-OUTPUT
             DISPLAY "CONSUMED MESSAGE LENGTH : " KAFKA-PAYLOAD-LEN
           END-IF.

       KAFKA-DSERIAL-MESSAGE.
      *****************************************************************
      * De-Serialize the Consumed message and convert it to Json format
      * using IXYSDSEA.
      *****************************************************************
           MOVE 'C'        TO SERDES-ACTION OF DESERIAL-AVRO-INPUT
           MOVE 'emp-schema' TO SCHEMA-NAME OF DESERIAL-AVRO-INPUT
           COMPUTE WS-SCHEMALEN = FUNCTION LENGTH(FUNCTION TRIM
                                  (SCHEMA-NAME OF DESERIAL-AVRO-INPUT))
           MOVE LOW-VALUE TO SCHEMA-NAME OF 
                             DESERIAL-AVRO-INPUT(WS-SCHEMALEN + 1:)
           DISPLAY "Schema Name : " 
                SCHEMA-NAME OF DESERIAL-AVRO-INPUT(1:WS-SCHEMALEN)

           MOVE KAFKA-SER-MSG-ASCII TO SER-MSG-ASC          
           SET SER-PAYLOAD-64 OF DESERIAL-AVRO-INPUT TO
                            ADDRESS OF SER-MSG-ASC
           MOVE KAFKA-PAYLOAD-LEN TO
                 SER-MESSAGE-LEN OF DESERIAL-AVRO-INPUT

           DISPLAY "KAFKA SERIAL BEGIN"

           CALL DESERIAL-PGM    USING DESERIAL-AVRO-INPUT
                   RETURNING DESERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                           DESERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             PERFORM DESTROY-KAFKA-CONSUME
             PERFORM DESTROY-SERDES-DSERIAL
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             SET ADDRESS OF JSON-MSG-ASCII  TO
                 DSER-PAYLOAD-64 OF DESERIAL-AVRO-OUTPUT

             INITIALIZE JSON-LENGTH

             INSPECT JSON-MSG-ASCII TALLYING JSON-LENGTH
                   FOR CHARACTERS BEFORE X'00' 

             MOVE FUNCTION DISPLAY-OF (FUNCTION NATIONAL-OF
                        (JSON-MSG-ASCII(1:JSON-LENGTH) 1208) 1140)
                   TO WS-JSON-MSG
             DISPLAY " JSON MESSAGE : "  WS-JSON-MSG(1:JSON-LENGTH)
           END-IF.


       CONVERT-JSON-COPY.
      *****************************************************************
      * Convert the Message from Json to Cobol Copybook format using
      * Data Transformation Utility generated snippet and module.
      *****************************************************************
           MOVE JSON-MSG-ASCII (1:JSON-LENGTH) TO
             JTXT-1208(1:JSON-LENGTH)

           CALL CJ2C-PGM USING JSON-DATA
                         RETURNING EVENT-DATA.

           IF JSON-CODE NOT = 0
             DISPLAY "JSON CODE : " JSON-CODE
             PERFORM DESTROY-KAFKA-CONSUME
             PERFORM DESTROY-SERDES-DSERIAL
             MOVE 16 TO RETURN-CODE
             GOBACK
           END-IF.

       DISPLAY-COBOL-CONSUMED-DATA.
      *****************************************************************
      * Display the Consumed Data in the COBOL copybook format
      *****************************************************************
           DISPLAY "CONSUMED EVENT-DATA : "
           DISPLAY "employeeId : " FUNCTION TRIM(employeeId)
           DISPLAY "fullName : " FUNCTION TRIM(fullName)
           DISPLAY "isFullTime : " isFullTime
           DISPLAY "employeeLevel : " employeeLevel
           DISPLAY "dateOfJoining : " dateOfJoining
           DISPLAY "insuranceCoverage : " insuranceCoverage
           DISPLAY "annualSalary : " annualSalary
           DISPLAY "documentData : " FUNCTION TRIM(documentData)
           DISPLAY "securityToken : " securityToken
           DISPLAY "skills(1) : " FUNCTION TRIM(skills(1))
           DISPLAY "skills(2) : " FUNCTION TRIM(skills(2))
           DISPLAY "teamMemberIds(1) : " teamMemberIds(1)
           DISPLAY "teamMemberIds(2) : " teamMemberIds(2)
           DISPLAY "sessionHistory(1) : " sessionHistory(1)
           DISPLAY "sessionHistory(2) : " sessionHistory(2)
           DISPLAY "salaryHistory(1) : " salaryHistory(1)
           DISPLAY "salaryHistory(2) : " salaryHistory(2)
           DISPLAY "monthlyAllowances(1) : " monthlyAllowances(1)
           DISPLAY "monthlyAllowances(2) : " monthlyAllowances(2)
           DISPLAY "profilePicture(1) : "
                                  FUNCTION TRIM(profilePicture(1))
           DISPLAY "profilePicture(2) : "
                                  FUNCTION TRIM(profilePicture(2))
           DISPLAY "certificationName(1) : "
                                  FUNCTION TRIM(certificationName(1))
           DISPLAY "certificationScore(1) : " certificationScore(1)
           DISPLAY "certificationName(2) : "
                                  FUNCTION TRIM(certificationName(2))
           DISPLAY "certificationScore(2) : " certificationScore(2)
           DISPLAY "email : " FUNCTION TRIM(email)
           DISPLAY "phoneNumber : " FUNCTION TRIM(phoneNumber)
           DISPLAY "street : " FUNCTION TRIM(street)
           DISPLAY "city : " FUNCTION TRIM(city)
           DISPLAY "state : " FUNCTION TRIM(state)
           DISPLAY "country : " FUNCTION TRIM(country)
           DISPLAY "pincode : " FUNCTION TRIM(pincode).

       DESTROY-KAFKA-CONSUME.
      *****************************************************************
      * Delete the Kafka objects once all the messages are consumed
      *****************************************************************
           MOVE 'D'             TO KAFKA-ACTION OF CONSUMER-INPUT
           DISPLAY "KAFKA CONSUMER DESTROY BEGIN"

           CALL CONSUMER-PGM USING CONSUMER-INPUT
                             RETURNING CONSUMER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                          CONSUMER-OUTPUT)
             MOVE KAFKA-MSG-RESPONSE OF CONSUMER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG OF CONSUMER-OUTPUT)
           END-IF.

       DESTROY-SERDES-DSERIAL.
      *****************************************************************
      * Delete the Serdes objects once all the messages are consumed
      *****************************************************************
           MOVE 'D' TO SERDES-ACTION OF DESERIAL-AVRO-INPUT
           DISPLAY "KAFKA SERDES DESTROY BEGIN"

           CALL DESERIAL-PGM    USING DESERIAL-AVRO-INPUT
                   RETURNING DESERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                              DESERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF DESERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(SERDES-MSG OF DESERIAL-AVRO-OUTPUT)
           END-IF.

       END PROGRAM 'IXYCAV64'.