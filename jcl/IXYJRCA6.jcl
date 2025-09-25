//IXYJRCA6   JOB @@JOBCARD@@
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
//* This JCL is used to run the consumer program - IXYCAV64.
//* The following changes should be done prior to running this JCL:
//*
//* 1) Change @@JOBCARD@@ to a valid jobcard based on the environment
//*
//* 2) Change @@IXYHLQ@@ to the HLQ for the IBM Open Enterprise SDK
//*    for Apache Kafka. ex: IXY.V110
//*
//* 3) Change @@CEEHLQ@@ to the HLQ for Language Environment runtime
//*    libraries. ex: CEE
//*
//* 4) Change @@LIBPATH@@ in STDENV DD to the USS path of
//*    librdkakfka.so.1
//*
//********************************************************************
//SETPARM SET IXYHLQ=@@IXYHLQ@@,
//            CEEHLQ=@@CEEHLQ@@
//*
//********************************************************************
//* Run KAFKA IXYCAV64
//********************************************************************
//CONSUMER EXEC PGM=IXYCAV64,REGION=0M
//STEPLIB  DD DISP=SHR,DSN=&IXYHLQ..SIXYSAMP.LOAD
//         DD DISP=SHR,DSN=&IXYHLQ..SIXYLOAD
//         DD DISP=SHR,DSN=&CEEHLQ..SCEERUN2
//         DD DISP=SHR,DSN=&CEEHLQ..SCEERUN
//IGZOPTS DD *
AMODE3164
DISPLAYWRAP
/*
//SYSOUT   DD  SYSOUT=*
//SYSPRINT DD  SYSOUT=*
//CEEDUMP  DD  SYSOUT=*
//CEEOPTS  DD   *
POSIX(ON)
ENVAR(LIBPATH=@@LIBPATH@@)
/*
//TOPICFIL DD DSN=&IXYHLQ..SIXYCFG(IXYTCONF),DISP=SHR
//CCONFFIL DD DSN=&IXYHLQ..SIXYCFG(IXYCCONF),DISP=SHR
//SCONFFIL DD DSN=&IXYHLQ..SIXYCFG(IXYSCONF),DISP=SHR
//*****************************************************