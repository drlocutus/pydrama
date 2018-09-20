
from libc.stdlib cimport malloc, free
from libc.stdio cimport sprintf
from libc.string cimport memset, memcmp, strlen, strcpy

# need this to create strs from c bufs
cdef extern from "Python.h":
    object PyBytes_FromStringAndSize(char *s, Py_ssize_t len)

# i get tired of typing unsigned long
ctypedef unsigned long ulong

# essential defs, like DCONSTV for mess.h
cdef extern from "drama.h":
    pass

cdef extern from "status.h":
    # this doesn't have to match type exactly :)
    ctypedef int StatusType

cdef extern from "sds.h":
    enum:
        SDS_STRUCT
        SDS_CHAR
        SDS_UBYTE
        SDS_BYTE
        SDS_USHORT
        SDS_SHORT
        SDS_UINT
        SDS_INT
        SDS_FLOAT
        SDS_DOUBLE
        SDS_I64
        SDS_UI64

        SDS_C_NAMELEN
        SDS_C_MAXARRAYDIMS

        # TODO: SDS_WATCH as needed

    ctypedef long SdsIdType
    ctypedef long SdsCodeType

    # create new top-level obj (structure?)
    void SdsCreate(char *name, long nextra, char *extra,
            SdsIdType *id, StatusType *status)

    # get name, type, dims from an id
    void SdsInfo(SdsIdType id, char *name, SdsCodeType *code,
            long *ndims, ulong *dims, StatusType *status)

    # create new child of parent
    void SdsNew(SdsIdType parent_id, char *name, long nextra, char *extra,
            SdsCodeType code, long ndims, ulong *dims,
            SdsIdType *id, StatusType *status)

    # find structure field by index
    void SdsIndex(SdsIdType parent_id, long index,
            SdsIdType *id, StatusType *status)

    # find structure field by name
    void SdsFind(SdsIdType parent_id, char *name,
            SdsIdType *id, StatusType *status)

    # find item by path: TOP.CHILD.ARRAY[1,5]
    void SdsFindByPath(SdsIdType parent_id, char *path,
            SdsIdType *id, StatusType *status)

    # get raw data pointer for an item
    void SdsPointer(SdsIdType id, void **data,
            ulong *length, StatusType *status)

    # read data. offset is actually index into data array?  unclear.
    # yeah -- basically *(type*)data = *((type*)sdsdata + offset)
    # or memcpy(data, (type*)sdsdata + offset, bytes)
    void SdsGet(SdsIdType id, ulong bytes, ulong offset,
            void *data, ulong *actlen, StatusType *status)

    # same semantics as above
    void SdsPut(SdsIdType id, ulong bytes, ulong offset,
            void *data, StatusType *status)

    # get structure in array, indices same dimensionality as array.
    void SdsCell(SdsIdType array_id, long nindices, ulong *indices,
            SdsIdType *id, StatusType *status)

    # deletes any obj currently at index and replace with id
    void SdsInsertCell(SdsIdType array_id, long nindices, ulong *indices,
                SdsIdType id, StatusType *status)

    # delete an obj, id becomes bad.  cannot delete array elements.
    # is recursive, but doesn't call SdsFreeId for children?
    void SdsDelete(SdsIdType id, StatusType *status)

    # return id to pool of free id's so it can be reused.
    # call SdsDelete() first. what about all children id's?
    # are they taken care of by SdsDelete() internally?
    void SdsFreeId(SdsIdType id, StatusType *status)

    # get required bufsize for sparse export
    void SdsSize(SdsIdType id, ulong *bytes, StatusType *status)

    # get required bufsize for a full export
    void SdsSizeDefined(SdsIdType id, ulong *bytes, StatusType *status)

    # serialize to buffer, sparsely -- undef stuff left out.
    void SdsExport(SdsIdType id, ulong length,
            void *data, StatusType *status)

    # serialize to buffer fully -- undef stuff included (zeros?)
    void SdsExportDefined(SdsIdType id, ulong length,
            void *data, StatusType *status)

    # alloc new sds structure from exported data
    void SdsImport(void *data, SdsIdType *id, StatusType *status)

    # read/modify exported data in-place, cannot change structure
    void SdsAccess(void *data, SdsIdType *id, StatusType *status)

    # pull an object out to the top level
    void SdsExtract(SdsIdType id, StatusType *status)

    # push an object into an existing structure
    void SdsInsert(SdsIdType parent_id, SdsIdType id, StatusType *status)

    # make a top-level copy of an object
    void SdsCopy(SdsIdType id, SdsIdType *copy_id, StatusType *status)

    void SdsResize(SdsIdType id, long ndims, ulong *dims, StatusType *status)

    void SdsRename(SdsIdType id, char *name, StatusType *status)

    void SdsGetExtra(SdsIdType id, long len, char *extra,
                ulong *actlen, StatusType *status)

    void SdsPutExtra(SdsIdType id, long len, char *extra, StatusType *status)

    bint SdsIsDefined(SdsIdType id, StatusType *status)

    # TODO: other ops...as needed.

cdef extern from "sds_err.h":

    enum:
        SDS__UNDEFINED

cdef extern from "DitsSystem.h":
    pass
        
cdef extern from "DitsSds.h":

    enum:
        DITS_C_NAMELEN
        
        DITS_M_ARGUMENT
        DITS_M_REP_MON_LOSS
        DITS_M_SENDCUR

    ctypedef struct DitsNameType:
        char n[DITS_C_NAMELEN]

    ctypedef struct Dits___NetTransIdType:
        unsigned char _id[8]
    
    ctypedef enum DitsMsgType:
        DITS_MSG_OBEY
        DITS_MSG_KICK
        DITS_MSG_SETPARAM
        DITS_MSG_GETPARAM
        DITS_MSG_SIGNAL
        DITS_MSG_TIMEOUT
        DITS_MSG_MESSAGE
        DITS_MSG_ERROR
        DITS_MSG_TRIGGER
        DITS_MSG_TRANSFAIL
        DITS_MSG_COMPLETE
        DITS_MSG_LOAD
        DITS_MSG_GETPATH
        DITS_MSG_CONTROL
        DITS_MSG_MONITOR
        DITS_MSG_REQNOTIFY
        DITS_MSG_MGETPARAM
        DITS_MSG___KICKBULK
        DITS_MSG___KICKBULKWAIT

    ctypedef struct DitsGsokMessageType:
        int flags
        int argument
        DitsMsgType type
        Dits___NetTransIdType transid
        DitsNameType name

cdef extern from "DitsTypes.h":

    enum:
        DITS_MSG_M_MESSAGE
        DITS_MSG_M_ERROR
    
    ctypedef long DitsMsgMaskType
    
    void * DitsMalloc(int size)
    void DitsFree(void *ptr)

    ctypedef void(*DitsActionRoutineType)(StatusType *status)
    ctypedef void(*DitsSpawnCheckRoutineType)(void *client_data, StatusType *status)

    struct DitsDummyPathType:
        pass

    struct DitsDummyTransIdType:
        pass

    ctypedef DitsDummyPathType * DitsPathType
    ctypedef DitsDummyTransIdType * DitsTransIdType

    ctypedef struct DitsActionDetailsType:
        DitsActionRoutineType obey
        DitsActionRoutineType kick
        long code
        long flags
        DitsSpawnCheckRoutineType spawnCheck
        void *data
        char name[DITS_C_NAMELEN]

    enum DitsReasonEnum:
        DITS_REA_OBEY              # Obey message received
        DITS_REA_KICK              # Kick message received
        DITS_REA_RESCHED           # Action reschedule by timer expiry
        DITS_REA_TRIGGER           # Action triggered by subsidiary action
        DITS_REA_ASTINT            # Action triggered by signal handler
        DITS_REA_LOAD              # Action triggered by successful load
        DITS_REA_LOADFAILED        # Action triggered by load failure
        DITS_REA_MESREJECTED       # A message was rejected
        DITS_REA_COMPLETE          # Message completion
        DITS_REA_DIED              # Task died
        DITS_REA_PATHFOUND         # Path to a task was found
        DITS_REA_PATHFAILED        # Failed to get a path
        DITS_REA_MESSAGE           # DITS_MSG_MESSAGE message received
        DITS_REA_ERROR             # DITS_ERR_MESSAGE message received
        DITS_REA_EXIT              # A loaded task has exited
        DITS_REA_NOTIFY            # A notification returned
        DITS_REA_BULK_TRANSFERRED  # Bulk transfer progress notifications
        DITS_REA_BULK_DONE         # Bulk transfer done notification
    ctypedef DitsReasonEnum DitsReasonType

    enum DitsReqEnum:
        DITS_REQ_SLEEP
        DITS_REQ_END
        DITS_REQ_EXIT
        DITS_REQ_STAGE
    ctypedef DitsReqEnum DitsReqType
    
    enum DitsArgFlagEnum:
        DITS_ARG_NODELETE
        DITS_ARG_DELETE
        DITS_ARG_COPY
        DITS_ARG_FREEID
        DITS_ARG_READFREE
    ctypedef DitsArgFlagEnum DitsArgFlagType


cdef extern from "DitsOrphan.h":

    DitsActionRoutineType DitsPutOrphanHandler(DitsActionRoutineType rout,
                                               StatusType *status)


# circular defs
cdef extern from "DitsSys.h":
    ctypedef void (*DitsInputCallbackRoutineType)(void *client_data, StatusType *status)
    ctypedef void (*DitsWaitRoutineType)(void *client_data, StatusType *status)


# declarations copied from ditsaltin.c
cdef extern from "ditsaltin.h":
    enum:
        DITS_C_ALT_IN_MAX

        XtInputNoneMask
        XtInputReadMask
        XtInputWriteMask
        XtInputExceptMask

    ctypedef struct Dits___AltInElemType:
        DitsInputCallbackRoutineType routine
        long number
        int condition
        void * client_data
        int done

    cdef struct Dits___AltInStruct:
        # NOTE: skipping fd_set vars
        long exit_flag
        Dits___AltInElemType Array[DITS_C_ALT_IN_MAX]

cdef extern from "ditsmsg.h":
    
    void MyMsgForward(StatusType *status)

cdef extern from "DitsSys.h":

    enum:
        DITS_M_X_COMPATIBLE
        DITS_M_IMB_ROUND_ROBIN
        DITS_M_READ_MASK
        DITS_M_WRITE_MASK
        DITS_M_EXCEPT_MASK

    # source is a file descriptor,
    # condition is read/write/exc mask, pass in a long*.
    # to test, & condition with
    # XtInputReadMask, XtInputWriteMask, XtInputExceptMask
    void DitsGetXInfo(int *source, void **condition, StatusType *status)

    void DitsPutActions(long size, DitsActionDetailsType details[], StatusType *status)

    # actually a macro...
    void DitsMainLoop(StatusType *status)

    ctypedef Dits___AltInStruct *DitsAltInType

    void DitsAltInClear(DitsAltInType *altin, StatusType *status)

    void DitsAltInAdd(DitsAltInType *altin,
                      long source, int condition,
                      DitsInputCallbackRoutineType proc,
                      void *client_data, StatusType *status)

    void DitsAltInLoop(DitsAltInType *altin, StatusType *status)

    void DitsPutEventWaitHandler(DitsWaitRoutineType routine,
                                 void * client_data, StatusType *status)

    ulong DitsMsgAvail(StatusType *status)

    void DitsMsgReceive(long *exit_flag, StatusType *status)



cdef extern from "DitsInteraction.h":

    ctypedef struct DitsPathInfoType:
        int MessageBytes
        int MaxMessages
        int ReplyBytes
        int MaxReplies

    void DitsPathGet(char *name, char *node, int flags,
                     DitsPathInfoType *info, DitsPathType *path,
                     DitsTransIdType *transid, StatusType *status)
    
    void DitsLosePath(DitsPathType path, StatusType *status)

    void DitsInitiateMessage(long flags, DitsPathType path,
                             DitsTransIdType *transid,
                             DitsGsokMessageType *message,
                             StatusType *status)

    void DitsGetEntInfo(int namelen, char *name, DitsPathType *path,
                        DitsTransIdType *transid, DitsReasonType *reason,
                        StatusType *reasonstat, StatusType *status)
    
    DitsPathType DitsGetEntPath()

    void DitsTrigger(SdsIdType arg, StatusType *status)

    void DitsTaskFromPath(DitsPathType path, int namelen,
                          char *taskname, StatusType *status)

    DitsPathType DitsGetParentPath()
    
    void DitsObey(DitsPathType path, char *action, SdsIdType arg,
                  DitsTransIdType *transid, StatusType *status)
    
    void DitsKick(DitsPathType path, char *action, SdsIdType arg,
                  DitsTransIdType *transid, StatusType *status)
    
    void DitsGetParam(DitsPathType path, char *param,
                      DitsTransIdType *transid, StatusType *status)
    
    void DitsSetParam(DitsPathType path, char *param, SdsIdType arg,
                      DitsTransIdType *transid, StatusType *status)

    void DitsInterested(DitsMsgMaskType mask, StatusType *status)
    
    void DitsActionTransIdWait(int flags, DitsDeltaTimeType *delay,
                               DitsTransIdType transId,
                               int *count, StatusType *status)

cdef extern from "DitsFix.h":

    ctypedef struct DitsDeltaTimeType:
        pass

    SdsIdType DitsGetArgument()
    
    void DitsPutArgument(SdsIdType arg, DitsArgFlagType flag, StatusType *status)
    
    void DitsGetName(int namelen, char *action_name, StatusType *status)

    void DitsPutRequest(DitsReqType req, StatusType *status)
    
    void DitsDeltaTime(unsigned int secs, unsigned int microsecs, DitsDeltaTimeType *delay)

cdef extern from "DitsSignal.h":

    void DitsSignalByName(char * action, SdsIdType arg, StatusType *status)

cdef extern from "DitsParam.h":

    SdsIdType DitsGetParId()

cdef extern from "DitsMsgOut.h":

    void MsgOut(StatusType *status, char *fmt, ...)

cdef extern from "Sdp.h":

    void SdpUpdate(SdsIdType id, StatusType *status)

cdef extern from "DitsUtil.h":
    
    long DitsActIndexByName(char *name, long *index, StatusType *status)
    
    void DitsIsActionActive(long index, int *active, StatusType *status)

cdef extern from "Dits_Err.h":

    enum:
        DITS__APP_ERROR
        DITS__APP_TIMEOUT
        DITS__MON_STARTED
        DITS__MON_CHANGED
        DITS__NOTUSERACT
        DITS__INVARG
        DITS__INVPATH
        DITS__UNEXPMSG
        DITS__EXITHANDLER


cdef extern from "Ers.h":

    enum:
        ERS_M_NOFMT
        ERS_M_HIGHLIGHT
        ERS_M_BELL
        ERS_M_ALARM

    void ErsRep(int flags, StatusType *status, char *fmt, ...)
    void ErsOut(int flags, StatusType *status, char *fmt, ...)
    void ErsClear(StatusType *status)
    void ErsAnnul(StatusType *status)


cdef extern from "mess.h":
    int MessGetMsg(StatusType msgid, int flag, int buflen, char  *buffer)

cdef extern from "jit.h":
    void jitSetDefaults(int flags, double timeout,
                        int taskBuffer, int sendBuffer,
                        int recvBuffer, int selfBuffer,
                        StatusType *status)
    void jitAppInit(char *taskname, StatusType *status)
    int  jitStop(char *taskname, StatusType *status)

    void jitDelayRequest(double secs, StatusType *status)
    void jitObey(char *task, char *action, SdsIdType arg, int xml,
                DitsTransIdType *transid, StatusType *status)
    void jitKick(char *task, char *action, SdsIdType arg, int xml,
                DitsTransIdType *transid, StatusType *status)
    void jitMonitorParam(char *task, char *param,
                        DitsTransIdType *transid, StatusType *status)
    void jitMonitorCancel(char *task, long monid,
                          DitsTransIdType *transid, StatusType *status)

    void jitGetParam(char *task, char *param,
                     DitsTransIdType *transid, StatusType *status)
    void jitSetParam(char *task, char *param, SdsIdType value,
                     DitsTransIdType *transid, StatusType *status)

    void jitDebugSet(long debug, StatusType * status)
    void jitDebugSetFile(unsigned short debugToFile, StatusType * status)
    long jitDebugGet()
    unsigned short jitDebugGetFile()
    int jitDebug(int level, char *fmt, ...)

cdef extern from "jitXML.h":
    void jitXML2Sds(int len, char *buf, SdsIdType *id, StatusType *status)

# not in jit.h:
cdef extern void jitPutFacilities()


cdef extern from "tide.h":
    void tideInit(DitsAltInType *altin, char *filename, StatusType *status)
    void tideExit(StatusType *status)
    void tideSetParam(DitsPathType path, char *param, SdsIdType arg,
                      DitsTransIdType *transid, StatusType *status)
    void tidePathGet(char *name, char *node, int flags,
                     DitsPathInfoType *info, DitsPathType *path,
                     DitsTransIdType *transid, StatusType *status)


