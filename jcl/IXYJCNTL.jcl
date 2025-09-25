//IXYJCNTL JOB @@JOBCARD@@
//**********************************************************************
//*
//* Copyright IBM Corp. 2025
//*
//* Licensed under the Apache License, Version 2.0 (the "License");
//* you may not use this file except in compliance with the License.
//* You may obtain a copy of the License at
//* 
//*     http://www.apache.org/licenses/LICENSE-2.0
//* 
//* Unless required by applicable law or agreed to in writing,
//* software distributed under the License is distributed on an
//* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//* either express or implied. See the License for the specific
//* language governing permissions and limitations under the
//* License.
//*
//**********************************************************************
//*
//* This JCL is used to create link-edit cards for the sample
//* cobol user defined functions - IXYCA2E, IXYCE2A and IXYDLCB.
//*
//* Following changes should be done prior to running this JCL:
//*
//* 1) Change @@JOBCARD@@ to a valid jobcard based on the environment
//*
//* 2) Change all @@IXYHLQ@@ to the HLQ for the IBM Open Enterprise
//*    SDK for Apache Kafka. ex: IXY.V110
//*
//* 3) Change all @@UNIT@@ to unit type and @@VOL@@ to volume serial.
//*    These are optional.  If not required delete the lines.
//*
//********************************************************************
//SETPARM SET IXYHLQ=@@IXYHLQ@@
//*
//********************************************************************
//*  Create sample Control card library if not existing
//********************************************************************
//CRETOBJ  EXEC PGM=IDCAMS
//SYSOUT DD SYSOUT=*
//SYSPRINT DD SYSOUT=*
//SYSIN DD *
  PRINT IDS('@@IXYHLQ@@.SIXYSAMP.CNTL') COUNT(1)
  IF MAXCC EQ 12 THEN DO
     ALLOC -
        DSNAME('@@IXYHLQ@@.SIXYSAMP.CNTL')  -
        NEW CATALOG                           -
        SPACE(2,2) CYLINDERS                  -
        BLKSIZE(32720)                        -
        LRECL(80)                             -
        DSORG(PO)                             -
        UNIT(@@UNIT@@)                        -
        VOL(@@VOL@@)                          -
        RECFM(F,B)                            -
        DSNTYPE(LIBRARY)
     SET MAXCC=0
  END
/*
//IXYCA2E  EXEC PGM=IEBGENER
//SYSPRINT DD  SYSOUT=*
//SYSIN    DD  DUMMY
//SYSUT2   DD  DISP=SHR,
//   DSN=@@IXYHLQ@@.SIXYSAMP.CNTL(IXYCA2E)
//SYSUT1   DD  *
    NAME IXYCA2E(R)
/*
//IXYCE2A  EXEC PGM=IEBGENER
//SYSPRINT DD  SYSOUT=*
//SYSIN    DD  DUMMY
//SYSUT2   DD  DISP=SHR,
//   DSN=@@IXYHLQ@@.SIXYSAMP.CNTL(IXYCE2A)
//SYSUT1   DD  *
    NAME IXYCE2A(R)
/*
//IXYDLCB  EXEC PGM=IEBGENER
//SYSPRINT DD  SYSOUT=*
//SYSIN    DD  DUMMY
//SYSUT2   DD  DISP=SHR,
//   DSN=@@IXYHLQ@@.SIXYSAMP.CNTL(IXYDLCB)
//SYSUT1   DD  *
    NAME IXYDLCB(R)
/*
//IXYLGCB  EXEC PGM=IEBGENER
//SYSPRINT DD  SYSOUT=*
//SYSIN    DD  DUMMY
//SYSUT2   DD  DISP=SHR,
//   DSN=@@IXYHLQ@@.SIXYSAMP.CNTL(IXYLGCB)
//SYSUT1   DD  *
    NAME IXYLGCB(R)
/*
//IXYSCONV EXEC PGM=IEBGENER
//SYSPRINT DD  SYSOUT=*
//SYSIN    DD  DUMMY
//SYSUT2   DD  DISP=SHR,
//   DSN=@@IXYHLQ@@.SIXYSAMP.CNTL(IXYSCONV)
//SYSUT1   DD  *
    NAME IXYSCONV(R)
/*