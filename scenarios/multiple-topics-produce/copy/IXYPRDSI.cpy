      ******************************************************************
      *                                                                *
      * MODULE NAME = IXYPRDSI                                         *
      *                                                                *
      *     Licensed Materials - Property of IBM                       *
      *                                                                *
      *     "Restricted Materials of IBM"                              *
      *                                                                *
      *     PID 5655-KAF                                               *
      *                                                                *
      *     (C) Copyright IBM Corp. 2024, 2024, 2025                   *
      *                                                                *
      ******************************************************************
      **                  KAFKA PRODUCER COPYBOOK                     **
      ******************************************************************
      ** This copybook is used in producer program to pass the input  **
      ** parameters from the calling program. Sample usage of this is **
      ** provided in IXYPRD31, IXYPRD64, IXYPRO31 and IXYPRO64        **
      ** programs calling IXYSPRDS program.                           **
      ******************************************************************
            05 KAFKA-ACTION         PIC X(1).
               88 KAFKA-INIT        VALUE 'I'.
               88 KAFKA-PRODUCE     VALUE 'P'.
               88 KAFKA-DELETE      VALUE 'D'.
            05 CONFIG-DATA OCCURS 15 TIMES.
               10 CONFIG-NAME       PIC X(1024).
               10 CONFIG-VALUE      PIC X(1024).
            05 NUM-OF-PARMS         PIC S9(09) BINARY.
            05 KAFKA-TOPIC-NAME     PIC X(2049).
            05 KAFKA-TYPE-PC        PIC 9(18) BINARY.
            05 KAFKA-MESSAGE.
               10 KAFKA-PAYLOAD-PTR PIC 9(18) COMP-5.
               10 KAFKA-PAYLOAD     REDEFINES KAFKA-PAYLOAD-PTR
                                    USAGE POINTER.
            05 KAFKA-MESSAGE-31.
               10 KAFKA-PAYLOAD-31-PTR PIC 9(9) COMP-5.
               10 KAFKA-PAYLOAD-31  REDEFINES KAFKA-PAYLOAD-31-PTR
                                        USAGE POINTER-32.
            05 KAFKA-PAYLOAD-LEN    PIC S9(18) BINARY.
            05 PARTITION-VALUE      PIC S9(09) BINARY.
            05 MSGFLAGS-VALUE       PIC X(01).
            05 TIMEOUT-MS-VALUE     PIC S9(09) BINARY.
            05 DISABLE-LOG-CONV     PIC X(16).
            05 CALLER-31BIT         PIC X(01).
            05 SKIP-CONV            PIC X(01).
            05 KAFKA-TOPIC-NBR      PIC 9(08)  COMP-5.
               88  KAFKA-TOPIC-1               VALUE 1.
               88  KAFKA-TOPIC-2               VALUE 2.
               88  KAFKA-TOPIC-3               VALUE 3.
               88  KAFKA-TOPIC-4               VALUE 4.
               88  KAFKA-TOPIC-5               VALUE 5.