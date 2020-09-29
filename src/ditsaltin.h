/* ditsaltin.h
 * Declares #defs and structs that are only found in ditsaltin.c.
 * 
 *
Copyright (C) 2020 East Asian Observatory

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


#ifndef _DITSALTIN_H_
#define _DITSALTIN_H_


#include <DitsSystem.h>
#include <DitsSys.h>


#define DITS_C_ALT_IN_MAX 41


#define XtInputNoneMask   0L
#define XtInputReadMask   1L
#define XtInputWriteMask  2L
#define XtInputExceptMask 4L


typedef struct  {      /* An element in DitsAltInType  */
    DitsInputCallbackRoutineType routine;
    long int number;   /* Number of event flag or fd   */
    int condition;     /* condition we are waiting for */
    DVOIDP  client_data;
    int done;          /* Used when processing responses */
} Dits___AltInElemType;


struct Dits___AltInStruct {
#   ifdef VMS
        unsigned long EfMask;           /* VMS Event flag mask */
#   elif !(defined(WIN32))
        fd_set readfds;                 /* Unix/VxWorks fd sets */
        fd_set writefds;
        fd_set exceptfds;
#   endif
    long int exit_flag;                 /* Indicates exiting */
    Dits___AltInElemType Array[DITS_C_ALT_IN_MAX]; /* elements */
};


#endif

