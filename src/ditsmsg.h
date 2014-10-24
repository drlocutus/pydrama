/* ditsmsg.h
 * Declares things only found in ditsmsg.c.
 */

#ifndef _DITSMSG_H_
#define _DITSMSG_H_

#include <DitsSystem.h>
#include <DitsSys.h>

typedef struct {
    SdsIdType sdsId;               /* id of any alloc'd structure */
    int flags;                     /* message flags */
    Dits___ResponseType response;  /* response code */
} ResponseDetailsType;

void Dits___MsgRespond(ResponseDetailsType *respDetails,
                       IMP_TaskID *taskId,      /* id of lost task */
                       StatusType *status);

#endif
