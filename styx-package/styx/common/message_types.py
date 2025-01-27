from enum import IntEnum


class MessageType(IntEnum):
    RunFunRemote = 0
    RunFunRqRsRemote = 1
    SendExecutionGraph = 2
    Ack = 3
    ReceiveExecutionPlan = 4
    RegisterWorker = 5
    Synchronize = 6
    AriaCommit = 7
    SyncSequencers = 8
    AriaProcessingDone = 9
    InitSequencer = 10
    GetLocalTopics = 11
    AriaFallbackDone = 12
    AriaFallbackStart = 13
    Unlock = 14
    ReceiveSequence = 15
    ClientMsg = 16
    DeterministicReordering = 17
    SnapID = 18
    Heartbeat = 19
    InitRecovery = 20
    ReadyAfterRecovery = 21
    SyncCleanup = 22
    RemoteWantsToProceed = 23
    ChainAbort = 24
    AckCache = 25
    SnapMarker = 99
    AlignStart = 100
    AlignEnd = 101
