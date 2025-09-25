//IXYJSFNC JOB @@JOBCARD@@
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
//* This JCL is used to compile and link-edit the functions - IXYCA2E
//* IXYCE2A and IXYDLCB
//*
//* The following changes should be done prior to running this JCL:
//*
//* 1) Change @@JOBCARD@@ to a valid jobcard based on the environment
//*
//* 2) Change all @@IXYHLQ@@ to the HLQ for the IBM Open Enterprise
//*    SDK for Apache Kafka. ex: IXY.V110
//*
//* 3) Change @@CEEHLQ@@ to the HLQ for Language Environment runtime
//*    libraries. ex: CEE
//*
//* 4) Change @@IGYHLQ@@ to the HLQ for COBOL compiler library.
//*    ex: IGY
//*
//* 5) Change all @@UNIT@@ to unit type and @@VOL@@ to volume serial.
//*    These are optional.  If not required delete the lines.
//*
//********************************************************************
//SETPARM SET IXYHLQ=@@IXYHLQ@@,
//            CEEHLQ=@@CEEHLQ@@,
//            IGYHLQ=@@IGYHLQ@@
//*
//COMPUDF   PROC
//********************************************************************
//* Compile COBOL program
//********************************************************************
//COMPCOB   EXEC PGM=IGYCRCTL,REGION=0M,
// PARM=' OPTFILE'
//STEPLIB  DD DISP=SHR,DSN=&IGYHLQ..SIGYCOMP
//         DD DISP=SHR,DSN=&CEEHLQ..SCEERUN2
//         DD DISP=SHR,DSN=&CEEHLQ..SCEERUN
//SYSIN    DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP(&MEMBER)
//SYSLIN   DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP.OBJ(&MEMBER)
//SYSLIB   DD DISP=SHR,DSN=&IXYHLQ..SIXYCOPY
//         DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP
//SYSOPTF  DD DATA,DLM='/>'
 LP(64),DLL,RENT,PGMNAME(LONGMIXED),EXPORTALL
/>
//SYSUT1   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT2   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT3   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT4   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT5   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT6   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT7   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT8   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT9   DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT10  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT11  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT12  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT13  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT14  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSUT15  DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSMDECK DD UNIT=SYSALLDA,SPACE=(CYL,(10,10))
//SYSPRINT DD SYSOUT=*
//CEEDUMP  DD SYSOUT=*
//TRLOG    DD SYSOUT=*
//           PEND
//********************************************************************
//* LinkEdit COBOL object
//********************************************************************
//LINKUDF0 PROC
//LKEDCOB  EXEC PGM=IEWBLINK,
// PARM='LIST,MAP,AMODE=64,RENT,XREF,LET,DYNAM(DLL),CASE(MIXED)'
//SYSLIB   DD DISP=SHR,DSN=&CEEHLQ..SCEELKED
//         DD DISP=SHR,DSN=&CEEHLQ..SCEEBND2
//         DD DISP=SHR,DSN=&CEEHLQ..SCEELIB
//OBJECT   DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP.OBJ(&MEMBER)
//SYSLIN   DD DISP=SHR,DSN=&IXYHLQ..SIXYSDEF(IXYSDEF)
//         DD *
    INCLUDE SYSLIB(CELQV004)
    INCLUDE OBJECT
    ENTRY CELQSTRT
/*
//         DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP.CNTL(&MEMBER)
//SYSLMOD  DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP.LOAD
//SYSDEFSD DD DISP=SHR,DSN=&IXYHLQ..SIXYSDEF(&MEMBER)
//SYSUT1   DD UNIT=SYSALLDA,SPACE=(CYL,(1,1))
//SYSOUT   DD DUMMY
//SYSPRINT DD SYSOUT=*
//           PEND
//*
//********************************************************************
//*  Create sample load library if not existing
//********************************************************************
//CRETLOAD EXEC PGM=IDCAMS
//SYSOUT DD SYSOUT=*
//SYSPRINT DD SYSOUT=*
//SYSIN DD *
  PRINT IDS('@@IXYHLQ@@.SIXYSAMP.LOAD') COUNT(1)
  IF MAXCC EQ 12 THEN DO
     ALLOC -
        DSNAME('@@IXYHLQ@@.SIXYSAMP.LOAD')    -
        NEW CATALOG                           -
        SPACE(2,2) CYLINDERS                  -
        BLKSIZE(23200)                        -
        LRECL(0)                              -
        DSORG(PO)                             -
        UNIT(@@UNIT@@)                        -
        VOL(@@VOL@@)                          -
        RECFM(U)                              -
        DSNTYPE(LIBRARY)
     SET MAXCC=0
  END
/*
//*
//********************************************************************
//*  Create sample object library if not existing
//********************************************************************
//CRETOBJ  EXEC PGM=IDCAMS
//SYSOUT DD SYSOUT=*
//SYSPRINT DD SYSOUT=*
//SYSIN DD *
  PRINT IDS('@@IXYHLQ@@.SIXYSAMP.OBJ') COUNT(1)
  IF MAXCC EQ 12 THEN DO
     ALLOC -
        DSNAME('@@IXYHLQ@@.SIXYSAMP.OBJ')  -
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
//*
//CMPMEM1 EXEC COMPUDF,MEMBER=IXYCA2E
//LNKMEM1 EXEC LINKUDF0,MEMBER=IXYCA2E
//CMPMEM2 EXEC COMPUDF,MEMBER=IXYCE2A
//LNKMEM2 EXEC LINKUDF0,MEMBER=IXYCE2A
//CMPMEM3 EXEC COMPUDF,MEMBER=IXYDLCB
//LNKMEM3 EXEC LINKUDF0,MEMBER=IXYDLCB
//CMPMEM4 EXEC COMPUDF,MEMBER=IXYLGCB
//LNKMEM4 EXEC LINKUDF0,MEMBER=IXYLGCB
//CMPMEM5 EXEC COMPUDF,MEMBER=IXYSCONV
//LNKMEM5 EXEC LINKUDF0,MEMBER=IXYSCONV