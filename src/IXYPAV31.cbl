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
      * MAIN PROGRAM IXYPAV31
      ******************************************************************
      * This is a sample program compiled in 31 bit addressing mode.
      * This Program is used to convert the COBOL Copybook formatted
      * data to Json format, Convert the message from Json to Avro and
      * Produce Avro Mesaage to Kafka
      *
      * This program uses
      * a. PCONFFIL file to get the Configuration parameters needed.
      * b. TOPICFIL file to get the topic details.
      * c. SCONFFIL file to get the Serdes configuration details.
      * d. Initialize Kafka and Serdes Configurations using IXYSPRDS
      *    and IXYSSERA module with Init option.
      * e. Each record of COBOL structured data is converted to
      *    Json message using the data transformation utility
      *    generated code IXYPRJSN.
      * f. Json Message is converted to Avro and Serialization of Avro
      *    happens using IXYSSERA module.
      * g. Calls IXYSPRDS module and Produce the message to kafka
      *    using Produce Option.
      * h. Destroy the Kafka and Serdes objects using IXYSPRDS and
      *    IXYSSERA with Destroy option.
      *
      * The program should be modified with the following changes:
      * 1) The value of PART-VAL should be set to the target partition
      *    value.
      * 2) The value of MSGFLGS-VAL should be set to message flags
      *    value.
      * 3) The value of TIMEOUT-MS should be set to the maximum amount
      *    of time (in milliseconds) that the call will block waiting
      *    for events.
      * 4) TOPICFIL - This is the file which contains the topic details.
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the topic length crosses 2049 bytes.
      * 5) PCONFFIL - Kafka Producer Configuration file
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the Configuration length crosses 2049 bytes
      * 6) SCONFFIL - Serdes Configuration file
      *    Change structure, file description and use a different flat
      *    file instead of standard file from the library accordingly,
      *    if the Configuration length crosses 2049 bytes
      * 7) Change the Schema Name Accordingly and pass it to
      *    SCHEMA-NAME OF SERIAL-AVRO-INPUT.
      * 8) Change the Data Transformation Utility generated snippets
      *    accordingly. i.e., IXYPRJSN, IXYCASC and IXYVARSN
      * 9) Change the POPULATE-EVENT-DATA para to populate the 
      *    EVENT-DATA as per the COBOL structure and needs.
      ******************************************************************

       IDENTIFICATION DIVISION.
        PROGRAM-ID. 'IXYPAV31'.
       ENVIRONMENT DIVISION.
        INPUT-OUTPUT SECTION.
         FILE-CONTROL.
           SELECT PCONFFIL ASSIGN TO PCONFFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT SCONFFIL ASSIGN TO SCONFFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

           SELECT TOPICFIL ASSIGN TO TOPICFIL
           ORGANIZATION IS SEQUENTIAL
           ACCESS MODE  IS SEQUENTIAL
           FILE STATUS  IS WS-FILE-STATUS.

       DATA DIVISION.
        FILE SECTION.
         FD PCONFFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  KAFKA-PCONFIG-FILE.

         01 KAFKA-PCONFIG-FILE.
            05 KAFKA-PCONFIG-REC      PIC X(2049).

         FD SCONFFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  SERDES-CONFIG-FILE.

         01 SERDES-CONFIG-FILE.
            05 SERDES-CONFIG-REC      PIC X(2049).

         FD TOPICFIL
           RECORD CONTAINS 2049  CHARACTERS
           BLOCK  CONTAINS 20490 CHARACTERS
           RECORDING MODE  IS  F
           DATA RECORD     IS  TOPIC-DATA.

         01 TOPIC-DATA.
            05 TOPIC-DATA-REC     PIC X(2049).

        WORKING-STORAGE SECTION.

      ******************************************************************
      *  PRODUCER Values
      ******************************************************************
         01 PART-VAL            PIC S9(9)  BINARY VALUE -1.
         01 MSGFLGS-VAL         PIC X(01)  VALUE X'02'.
         01 TIMEOUT-MS          PIC S9(9)  BINARY VALUE 5000.
         01 TOPIC-LENGTH        PIC S9(4) BINARY VALUE 0000.
         01 WS-DISPLAY-ERR      PIC S9(9) SIGN IS LEADING SEPARATE.
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

      * Input values for Producer program
         01 PRODUCER-INPUT.
            COPY IXYPRDSI.
      * Output values for Producer program
         01 PRODUCER-OUTPUT.
            COPY IXYPRDSO.
         01 PRODUCER-PGM        PIC X(8) VALUE "IXYSPRDS".

      * Input/Output values for Serialization program
         01 SERIAL-AVRO-INPUT.
            COPY IXYSERAI.
         01 SERIAL-AVRO-OUTPUT.
            COPY IXYSERAO.
         01 SERIAL-PGM          PIC X(8) VALUE "IXYSSERA".

      ******************************************************************
      * COBOL DATA COPYBOOK GENERATED BY DATA TRANSFORMATION UTILITY
      ******************************************************************
         COPY IXYCASC.
         COPY IXYVARSN.

       PROCEDURE DIVISION.
           DISPLAY "DTUS AMODE 31 PRODUCER PROGRAM"
           PERFORM READ-PRODUCER-CONFIG
           PERFORM READ-PRODUCER-TOPIC
           PERFORM READ-SERDES-CONFIG
           PERFORM INIT-KAFKA-PRODUCER
           PERFORM INIT-SERDES-SERIAL
           PERFORM POPULATE-EVENT-DATA
           PERFORM CONVERT-COPY-JSON
           PERFORM KAFKA-SERIAL-MESSAGE
           PERFORM KAFKA-PRODUCE-MESSAGE
           PERFORM DESTROY-KAFKA-PRODUCE
           PERFORM DESTROY-SERDES-SERIAL
           GOBACK
           .

       READ-PRODUCER-CONFIG.
      *****************************************************************
      * PCONFFIL contains the Configuration Parameters which are needed
      * for setting up the KAFKA connection. Configuration file is read
      * and parsed to extract the configuration Parameter and its
      * value. Length of Configuration Parameter and its value is
      * determined. End of string (LOW VALUES) is appended to the
      * configuration parameter and value. This file can contain
      * comments starting with '#'. Parameter and Value is delimited
      * by '='.
      *****************************************************************
           OPEN INPUT PCONFFIL
           PERFORM UNTIL WS-EOF
             READ PCONFFIL
             AT END SET WS-EOF TO TRUE
             NOT AT END
               IF KAFKA-PCONFIG-REC(1:1) NOT = '#'

                 MOVE 0 TO WS-DELIMITER-POS

                 INSPECT KAFKA-PCONFIG-REC TALLYING WS-DELIMITER-POS
                   FOR CHARACTERS BEFORE WS-DELIMITER

                 IF WS-DELIMITER-POS NOT = 0
                   MOVE KAFKA-PCONFIG-REC(1:WS-DELIMITER-POS) TO
                                       KAFKA-CONFIG-PARM
                   MOVE KAFKA-PCONFIG-REC(WS-DELIMITER-POS + 2:) TO
                                       KAFKA-CONFIG-VALUE
                 END-IF

                 ADD 1 TO NUM-OF-PARMS OF PRODUCER-INPUT
                 ADD 1 TO WS-CNT

                 COMPUTE WS-PARMLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-PARM))
                 COMPUTE WS-VALLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                 MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                             CONFIG-NAME OF
                             PRODUCER-INPUT(WS-CNT)(1:WS-PARMLEN)
                 MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                             CONFIG-VALUE OF
                              PRODUCER-INPUT(WS-CNT)(1:WS-VALLEN)

      * End of string identified using LOW VALUE in C. Hence appending
      * it to the end of each configuration and its parameters
                 MOVE LOW-VALUE TO CONFIG-NAME
                    OF PRODUCER-INPUT(WS-CNT)(WS-PARMLEN + 1:)
                 MOVE LOW-VALUE TO CONFIG-VALUE
                    OF PRODUCER-INPUT(WS-CNT)(WS-VALLEN + 1:)
               END-IF
              END-READ
           END-PERFORM

           CLOSE PCONFFIL.

       READ-PRODUCER-TOPIC.
      *****************************************************************
      * TOPICFIL is used to pass the Topic name to Kafka. Only
      * one topic name is being supported currently. Topic name should
      * be of maximum 2049 bytes. If its more than 2049 bytes, please
      * update the file description and use a different file
      * instead of standard file from the library.
      *****************************************************************
           OPEN INPUT TOPICFIL

           READ TOPICFIL

           INSPECT TOPIC-DATA-REC TALLYING TOPIC-LENGTH
                   FOR CHARACTERS BEFORE ' '

           MOVE FUNCTION TRIM(TOPIC-DATA-REC) TO
                      KAFKA-TOPIC-NAME OF
                        PRODUCER-INPUT(1:TOPIC-LENGTH)
           MOVE LOW-VALUE TO KAFKA-TOPIC-NAME OF
                        PRODUCER-INPUT(TOPIC-LENGTH + 1:)
           DISPLAY "Producer Topic : " KAFKA-TOPIC-NAME OF
                              PRODUCER-INPUT(1:TOPIC-LENGTH)

           CLOSE TOPICFIL.

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

                 ADD 1 TO NUM-OF-PARMS OF SERIAL-AVRO-INPUT
                 ADD 1 TO WS-CNT

                 COMPUTE WS-PARMLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-PARM))
                 COMPUTE WS-VALLEN = FUNCTION LENGTH(
                             FUNCTION TRIM(KAFKA-CONFIG-VALUE))

                 MOVE FUNCTION TRIM(KAFKA-CONFIG-PARM) TO
                             CONFIG-NAME OF
                             SERIAL-AVRO-INPUT(WS-CNT)(1:WS-PARMLEN)
                 MOVE FUNCTION TRIM(KAFKA-CONFIG-VALUE) TO
                             CONFIG-VALUE OF
                              SERIAL-AVRO-INPUT(WS-CNT)(1:WS-VALLEN)

      * End of string identified using LOW VALUE in C. Hence appending
      * it to the end of each configuration and its parameters
                 MOVE LOW-VALUE TO CONFIG-NAME
                    OF SERIAL-AVRO-INPUT(WS-CNT)(WS-PARMLEN + 1:)
                 MOVE LOW-VALUE TO CONFIG-VALUE
                    OF SERIAL-AVRO-INPUT(WS-CNT)(WS-VALLEN + 1:)
               END-IF
              END-READ
           END-PERFORM

           CLOSE SCONFFIL.

       INIT-KAFKA-PRODUCER.
      **************** Initialisation section Begin *******************
      * Invoke the Producer program to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the config file
      *****************************************************************

      * KAFKA-TYPE-PC is 0 for PRODUCER
      * KAFKA-TYPE-PC is 1 for CONSUMER
           MOVE 0               TO KAFKA-TYPE-PC OF PRODUCER-INPUT
           MOVE PART-VAL        TO PARTITION-VALUE OF PRODUCER-INPUT
           MOVE MSGFLGS-VAL     TO MSGFLAGS-VALUE OF PRODUCER-INPUT
           MOVE TIMEOUT-MS      TO TIMEOUT-MS-VALUE OF PRODUCER-INPUT
           MOVE 'I'             TO KAFKA-ACTION OF PRODUCER-INPUT
           DISPLAY "KAFKA PRODUCER INIT BEGIN"

           CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                           PRODUCER-OUTPUT)
             MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG OF PRODUCER-OUTPUT)
           END-IF.

       INIT-SERDES-SERIAL.
      *****************************************************************
      * Invoke the Serialize module to Initialise the configuration
      * Parameters. This is done after all the configuration
      * parameters are read from the Serdes config file
      *****************************************************************
           MOVE 'I'    TO SERDES-ACTION OF SERIAL-AVRO-INPUT

           DISPLAY "KAFKA SERDES INIT BEGIN"

           CALL SERIAL-PGM    USING SERIAL-AVRO-INPUT
                   RETURNING SERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                           SERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             PERFORM DESTROY-KAFKA-PRODUCE
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY FUNCTION TRIM(SERDES-MSG OF SERIAL-AVRO-OUTPUT)
           END-IF.

       POPULATE-EVENT-DATA.
      *****************************************************************
      * Populate EVENT-DATA Copybook structure data
      * Currently its hard coded few dummy values and this can be
      * either read from file/DB2 or from other application program
      *****************************************************************
           INITIALIZE EVENT-DATA
           MOVE 1 TO employeeId
           MOVE "Employee 1" TO fullName
           MOVE 'Y' TO isFullTime
           MOVE 000000001 TO employeeLevel
           MOVE 07012020 TO dateOfJoining
           MOVE 1000.50 TO insuranceCoverage
           MOVE 1000.10 TO annualSalary
           MOVE 'ABCDE' TO documentData
           MOVE 'abcde1234567890z' TO securityToken
           MOVE "COBOL" to skills(1)
           MOVE "PL1" to skills(2)
           MOVE 100000001 TO teamMemberIds(1)
           MOVE 200000001 TO teamMemberIds(2)
           MOVE 900001 To sessionHistory(1)
           MOVE 8000000001 to sessionHistory(2)
           MOVE 500.50 to salaryHistory(1)
           MOVE 700.10 to salaryHistory(2)
           MOVE 50.00 to monthlyAllowances(1)
           MOVE 100.00 to monthlyAllowances(2)
           MOVE 'employee' TO profilePicture(1)
           MOVE 'candidate' TO profilePicture(2)
           MOVE 'JCL certificate' TO certificationName(1)
           MOVE 90 to certificationScore(1)
           MOVE 'DB2 certificate' TO certificationName(2)
           MOVE 80 to certificationScore(2)
           MOVE "xyz@abc.com" to email
           MOVE '1234567890' TO phoneNumber
           MOVE 'ABC' TO street
           MOVE 'XYZ' TO city
           MOVE 'efg' to state
           MOVE 'hij' to country
           MOVE '123456' to pincode
           DISPLAY "EVENT DATA TO BE PRODUCED"
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
           DISPLAY "pincode : " FUNCTION TRIM(pincode)             
           .

       CONVERT-COPY-JSON.
      *****************************************************************
      * Convert the Cobol Copybook format to Json using
      * Data Transformation Utility generated snippet as Copybook.
      *****************************************************************
           DISPLAY "CONVERT COPYBOOK TO JSON BEGIN"

           COPY IXYPRJSN.

           IF JSON-CODE NOT = 0
             DISPLAY "JSON CODE : " JSON-CODE
             PERFORM DESTROY-KAFKA-PRODUCE
             PERFORM DESTROY-SERDES-SERIAL
             MOVE 16 TO RETURN-CODE             
             GOBACK
           ELSE
             MOVE FUNCTION DISPLAY-OF (FUNCTION NATIONAL-OF
                                    (JTXT-1208 1208) 1140)
                   TO WS-JSON-MSG
             DISPLAY " JSON MESSAGE : "  WS-JSON-MSG(1:JSON-LENGTH)
           END-IF.

       KAFKA-SERIAL-MESSAGE.
      *****************************************************************
      * Convert the Json Message to Avro and Serialize the message
      * using IXYSSERA.
      *****************************************************************
           MOVE 'P'        TO SERDES-ACTION OF SERIAL-AVRO-INPUT
           MOVE 'Y'        TO CALLER-31BIT OF SERIAL-AVRO-INPUT
           MOVE 'emp-schema' TO SCHEMA-NAME OF SERIAL-AVRO-INPUT

           COMPUTE WS-SCHEMALEN = FUNCTION LENGTH(FUNCTION TRIM
                                  (SCHEMA-NAME OF SERIAL-AVRO-INPUT))
           MOVE LOW-VALUE TO SCHEMA-NAME OF 
                             SERIAL-AVRO-INPUT(WS-SCHEMALEN + 1:)
           DISPLAY "Schema Name : " 
                SCHEMA-NAME OF SERIAL-AVRO-INPUT(1:WS-SCHEMALEN)

           MOVE LOW-VALUE TO JTXT-1208(JSON-LENGTH + 1:)
           SET JSON-PAYLOAD-31 OF  SERIAL-AVRO-INPUT TO
             ADDRESS OF JTXT-1208(1:JSON-LENGTH)

           DISPLAY "KAFKA SERIAL BEGIN"

           CALL SERIAL-PGM    USING SERIAL-AVRO-INPUT
                   RETURNING SERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                           SERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             PERFORM DESTROY-KAFKA-PRODUCE
             PERFORM DESTROY-SERDES-SERIAL             
             MOVE 16 TO RETURN-CODE
             GOBACK
           ELSE
             DISPLAY "Payload Size : "
               SER-PAYLOAD-SIZE OF SERIAL-AVRO-OUTPUT
           END-IF.


       KAFKA-PRODUCE-MESSAGE.
      *****************************************************************
      * The Serialized message is passed to the Producer Program to
      * Produce a Kafka event
      *****************************************************************

           MOVE 'P'        TO KAFKA-ACTION OF PRODUCER-INPUT
           SET KAFKA-PAYLOAD-31 OF PRODUCER-INPUT TO
             SER-PAYLOAD-31 OF SERIAL-AVRO-OUTPUT

           MOVE SER-PAYLOAD-SIZE OF SERIAL-AVRO-OUTPUT TO
              KAFKA-PAYLOAD-LEN OF PRODUCER-INPUT
           MOVE 'Y' TO CALLER-31BIT OF PRODUCER-INPUT
           MOVE 'Y' TO SKIP-CONV OF PRODUCER-INPUT
           DISPLAY "KAFKA PRODUCE BEGIN"

           CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                                    PRODUCER-OUTPUT)
             MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG OF PRODUCER-OUTPUT)
           END-IF.

       DESTROY-KAFKA-PRODUCE.
      *****************************************************************
      * Delete the Kafka objects once all the messages are produced
      *****************************************************************
           MOVE 'D'             TO KAFKA-ACTION OF PRODUCER-INPUT
           DISPLAY "KAFKA PRODUCER DESTROY BEGIN"

           CALL PRODUCER-PGM    USING PRODUCER-INPUT
                   RETURNING PRODUCER-OUTPUT

           IF KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(KAFKA-MSG OF
                                              PRODUCER-OUTPUT)
             MOVE KAFKA-MSG-RESPONSE OF PRODUCER-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(KAFKA-MSG OF PRODUCER-OUTPUT)
           END-IF.

       DESTROY-SERDES-SERIAL.
      *****************************************************************
      * Delete the Serdes objects once all the messages are produced
      *****************************************************************
           MOVE 'D' TO SERDES-ACTION OF SERIAL-AVRO-INPUT
           DISPLAY "KAFKA SERDES DESTROY BEGIN"

           CALL SERIAL-PGM    USING SERIAL-AVRO-INPUT
                   RETURNING SERIAL-AVRO-OUTPUT

           IF SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT NOT = 0
             DISPLAY "ERROR : " FUNCTION TRIM(SERDES-MSG OF
                                              SERIAL-AVRO-OUTPUT)
             MOVE SERDES-MSG-RESPONSE OF SERIAL-AVRO-OUTPUT TO
                                       WS-DISPLAY-ERR
             DISPLAY "ERROR CODE : " WS-DISPLAY-ERR
             MOVE 16 TO RETURN-CODE
           ELSE
             DISPLAY FUNCTION TRIM(SERDES-MSG OF SERIAL-AVRO-OUTPUT)
           END-IF.

       END PROGRAM 'IXYPAV31'.