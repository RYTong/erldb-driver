/**
 * Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
 * All rights reserved.
 *
 * The contents of this file are subject to the Erlang Database Driver
 * Public License Version 1.0, (the "License"); you may not use this 
 * file except in compliance with the License. You should have received
 * a copy of the Erlang Database Driver Public License along with this
 * software. If not, it can be retrieved via the world wide web at
 * http://www.rytong.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 */

#include "DB2Utils.h"

namespace rytong {

void getDiagnostics(SQLCHAR*, SQLSMALLINT, SQLHANDLE);

bool handleInfo(char* OutBuffer,   /* message output buffer */
                int bufferLength,  /* length of output buffer */
                SQLSMALLINT htype, /* handle type identifier */
                SQLHANDLE hndl,    /* handle used by the CLI function */
                SQLRETURN cliRC,   /* return code of the CLI function */
                int line,
                char *file)
{
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH + 1] = {0};
    
    switch (cliRC) {
        case SQL_SUCCESS:
            return true;

        case SQL_INVALID_HANDLE:
            strcpy((char*)message, "CLI INVALID HANDLE");
            break;
            
        case SQL_ERROR:
            getDiagnostics(message, htype, hndl);
            break;
            
        case SQL_SUCCESS_WITH_INFO:
            return true;
            
        case SQL_STILL_EXECUTING:
            return true;
        
        case SQL_NEED_DATA:
            return true;
        
        case SQL_NO_DATA_FOUND:
            return true;

        default:
            strcpy((char*)message, "UNKNOWN ERROR");
    }

    if (OutBuffer != NULL) {
        strncpy(OutBuffer, (char*)message, bufferLength);
    }

    SysLogger::error("\n\rcliRC = %d\n\rline  = %d\n\rfile  = %s\n\r", cliRC, line, file);
    return false;
}

void getDiagnostics(SQLCHAR* message, SQLSMALLINT htype, SQLHANDLE hndl)
{
    SQLCHAR sqlstate[SQL_SQLSTATE_SIZE + 1];
    SQLINTEGER sqlcode;
    SQLSMALLINT length, i;

    i = 1;

    while(SQLGetDiagRec(htype,
                        hndl,
                        i,
                        sqlstate,
                        &sqlcode,
                        message,
                        SQL_MAX_MESSAGE_LENGTH + 1,
                        &length) == SQL_SUCCESS)
    {
        SysLogger::error("\n\r  SQLSTATE          = %s\n\r  Native Error "
                         "Code = %d\n\r%s\n\r", sqlstate, sqlcode, message);
        i++;
    }
}

/* calculate enough sigle-byte length */
int calcSGByteLength(char* erlBuf, int* pIndex)
{
    int type, size;

    if (ei_get_type(erlBuf, pIndex, &type, &size) == 0) {
        return size == 0 ? 64:size + 1;
    } else {
        return 0;
    }
}

/* calculate enough double-byte length */
int calcDBByteLength(char* erlBuf, int* pIndex)
{
    return calcSGByteLength(erlBuf, pIndex) * 2;
}

bool decodeBinary(char* erlBuf, int* pIndex, char* outBuf, int bufLen, long* outLen)
{
    int type, size;
    long long integerValue = 0;
    double doubleValue = 0;

    ei_get_type(erlBuf, pIndex, &type, &size);

    if (bufLen >= size && ei_decode_binary(erlBuf, pIndex, outBuf, outLen) == 0) {
    } else if (bufLen > size && ei_decode_string(erlBuf, pIndex, outBuf) == 0) {
        *outLen = strlen(outBuf);
    } else if (ei_decode_longlong(erlBuf, pIndex, &integerValue) == 0) {
        snprintf(outBuf, bufLen, "%lld", integerValue);
        *outLen = strlen(outBuf);
    } else if (ei_decode_double(erlBuf, pIndex, &doubleValue) == 0) {
        snprintf(outBuf, bufLen, "%lf", doubleValue);
        *outLen = strlen(outBuf);
    } else {
        return false;
    }

    return true;
}

bool decodeString(char* erlBuf, int* pIndex, char* outBuf, int bufLen, long* outLen)
{
    int type, size;
    long long integerValue = 0;
    double doubleValue = 0;

    ei_get_type(erlBuf, pIndex, &type, &size);

    if (bufLen > size && ei_decode_binary(erlBuf, pIndex, outBuf, outLen) == 0) {
        outBuf[*outLen] = '\0';
    } else if (bufLen > size && ei_decode_string(erlBuf, pIndex, outBuf) == 0) {
        *outLen = strlen(outBuf);
    } else if (ei_decode_longlong(erlBuf, pIndex, &integerValue) == 0) {
        snprintf(outBuf, bufLen, "%lld", integerValue);
        *outLen = strlen(outBuf);
    } else if (ei_decode_double(erlBuf, pIndex, &doubleValue) == 0) {
        snprintf(outBuf, bufLen, "%lf", doubleValue);
        *outLen = strlen(outBuf);
    } else {
        return false;
    }

    return true;
}

bool decodeInteger(char* erlBuf, int* pIndex, long long* outValue)
{
    long len = 0;
    int type, size;
    double doubleValue = 0;
    char strValue[64];

    if(ei_decode_longlong(erlBuf, pIndex, outValue) == 0) {
    } else if (ei_decode_double(erlBuf, pIndex, &doubleValue) == 0) {
        *outValue = (long long)doubleValue;
    } else if (ei_get_type(erlBuf, pIndex, &type, &size) == 0 && size < 64){
        if (ei_decode_string(erlBuf, pIndex, strValue) == 0) {
        } else if (ei_decode_binary(erlBuf, pIndex, strValue, &len) == 0) {
            strValue[len] = '\0';
        }
        if (sscanf(strValue, "%lld", outValue) < 1) {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

bool decodeDouble(char* erlBuf, int* pIndex, double* outValue)
{
    long len = 0;
    int type, size;
    long long integerValue = 0;
    char strValue[64];

    if(ei_decode_double(erlBuf, pIndex, outValue) == 0) {
    } else if (ei_decode_longlong(erlBuf, pIndex, &integerValue) == 0) {
        *outValue = (double)integerValue;
    } else if (ei_get_type(erlBuf, pIndex, &type, &size) == 0 && size < 64){
        if (ei_decode_string(erlBuf, pIndex, strValue) == 0) {
        } else if (ei_decode_binary(erlBuf, pIndex, strValue, &len) == 0) {
            strValue[len] = '\0';
        }
        if (sscanf(strValue, "%lf", outValue) < 1) {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

bool decodeDBString (char* erlBuf, int* pIndex, unsigned char* outBuf, int bufLen, long* outLen)
{
    int type, size;
    long long integerValue;

    if (ei_decode_list_header(erlBuf, pIndex, &size) == 0 && size < bufLen /2) {
        for (int i = 0; i < size; ++i) {
            if (decodeInteger(erlBuf, pIndex, &integerValue)) {
                outBuf[i * 2] = ((unsigned short)integerValue) >> 8;
                outBuf[i * 2 + 1] = ((unsigned short)integerValue) & 0x00ff;
            } else {
                return false;
            }
        }
        ((short*)outBuf)[size] = 0;
        *outLen  = size * 2;
        ei_decode_list_header(erlBuf, pIndex, &size);
    } else if (ei_get_type(erlBuf, pIndex, &type, &size) == 0 &&
            size < bufLen / 2 &&
            ei_decode_string(erlBuf, pIndex, (char*)outBuf) == 0) {
        for (int i = size; i >= 0; --i) {
            outBuf[i * 2 + 1] = outBuf[i];
            outBuf[i * 2] = 0;
        }
        *outLen = size * 2;
    } else {
        return false;
    }

    return true;
}

}/* end of namespace rytong */
