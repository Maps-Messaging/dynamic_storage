# Partition write safety (MSG-276)

The version-1 file headers, data frame layout and 24-byte index slots are unchanged. Existing readers can consume completed records written by this change; existing stores need no migration. The historical frame length field remains payload length plus four, while the index length includes the complete frame header. Buffer positions are respected and caller buffers are not consumed.

An append loops until all expected bytes are written, checks that no buffers remain and verifies file growth. Sixteen consecutive zero-byte writes fail rather than spin. An IOException (including ENOSPC) rolls back only the new tail to its original EOF. After successful rollback, callers may retry once space is available. If truncation/position reset fails, later appends on that instance are rejected: reopen with recovery validation before resuming. An incomplete append never returns an index record.

Index metadata is populated before publishing the position field. Space/read-only checks happen before mutation. In sync mode the mapped invalidation and metadata are forced before publishing position, then the published position is forced; the data channel already uses DSYNC. A publication exception rejects further appends on the IndexStorage instance. It may leave complete unindexed data or an ambiguously committed entry; it must not be blindly retried in that instance.

This preserves the existing serialized-access requirement. It does not introduce concurrent-reader guarantees. Async mode does not gain power-loss durability from a Java memory fence: operating-system writeback may reorder dirty pages. Even sync mode relies on filesystem/device force guarantees; a 24-byte slot is not a hardware-atomic transaction. Recovery validation remains separate. Sync publication adds mapped flushes and requires performance testing before rollout.

Tests: FrameAppenderTest invokes dependency-free checks for short writes, legacy frame bytes, zero progress, simulated ENOSPC before/after partial output, retry after rollback, rollback failure, and real-file append. IndexPublicationTest checks legacy bytes, preflight failures and mapped sync publication/deletion after reopen. These fault-injection tests do not replace a real full-filesystem test or power-cut testing.

Run: `mvn -Dtest=FrameAppenderTest,IndexPublicationTest test`. Also run the full existing storage suite and a bounded-filesystem ENOSPC integration test before merging. No recovery logic, compaction policy, or server backpressure policy is changed here.
