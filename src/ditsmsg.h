/* ditsmsg.h
 * Declares things only found in ditsmsg.c.
 */

#ifndef _DITSMSG_H_
#define _DITSMSG_H_

#include <DitsSystem.h>
#include <DitsSys.h>
#include <DitsUtil.h>
#include <Dits_Err.h>

//typedef struct {
    //SdsIdType sdsId;               /* id of any alloc'd structure */
    //int flags;                     /* message flags */
    //Dits___ResponseType response;  /* response code */
//} ResponseDetailsType;

//void Dits___MsgRespond(ResponseDetailsType *respDetails,
                       //IMP_TaskID *taskId,      /* id of lost task */
                       //StatusType *status);

/* NOTE
 * Dits___MsgRespond is a local/inline function, so we
 * can't actually use it.  Relies heavily on TaskCurrent macro,
 * so I guess I'll reimpl here as my own inline function.
 * Only does the DITS_RESP_FORWARD bit, no logging,
 * might return with bad status.
 */
inline void MyMsgForward(StatusType *status)
{
    if(*status != STATUS__OK)
        return;
    
    if(TaskCurrent(Mess).reason != DITS_REA_MESSAGE
       && TaskCurrent(Mess).reason != DITS_REA_ERROR)
    {
        *status = DITS__UNEXPMSG;
        return;
    }
    
    int actptr = TaskCurrent(Mess).transid->actionptr;
    if(actptr < 0)
    {
        *status = DITS__NOTUSERACT;
        return;
    }
    
    TaskCurrent(replyDetails).path=TaskActionArray[actptr].path;
    TaskCurrent(replyDetails).tag=TaskActionArray[actptr].messageTag;
    TaskCurrent(replyDetails).transid=TaskActionArray[actptr].transid;
   
    Dits___TapMessageType m;
    m.reasonstat = STATUS__OK;
    m.flags = DITS_M_ARGUMENT;
    m.argument = TaskCurrent(Mess).argin;
    m.type = (TaskCurrent(Mess).reason == DITS_REA_MESSAGE) ?
                    DITS_MSG_MESSAGE : DITS_MSG_ERROR;
    m.transid = TaskActionArray[actptr].transid;
    Dits___SendTap(0,
                   TaskActionArray[actptr].path,
                   TaskActionArray[actptr].messageTag,
                   &m,status);
    
    /*
     *  If the tap message failed due to lack of space then try to
     *  send a message without the argument, with status INFOSENDERR.
     */
    if (*status == IMP__CANT_FIT)
    {
        *status = STATUS__OK;
        m.argument = 0;
        m.flags = 0;
        m.reasonstat = DITS__INFOSENDERR;
        Dits___SendTap(DITS_M_PRIORITY,
                       TaskActionArray[actptr].path,
                       TaskActionArray[actptr].messageTag,
                       &m,status);
    }
    
    /* 
     * Clear path and return with whatever status.
     */
    TaskCurrent(replyDetails).path = NIL;
}

#endif
