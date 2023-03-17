package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        //A commit record should be emitted
        LogRecord record = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN  = logManager.appendToLog(record);

        //the log should be flushed
        this.logManager.flushToLSN(LSN);

        //the transaction table and the transaction status should be updated
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        //An abort record should be emitted
        LogRecord record = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN  = logManager.appendToLog(record);

        //the transaction table and transaction status should be updated
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);

        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        //Any changes that need to be undone should be undone
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING){
            LogRecord record = this.logManager.fetchLogRecord(prevLSN);
            //Optional<Long> CLRPageNum = Optional.empty();

            while(true){
                if (record != null && record.isUndoable()){
                    Pair<LogRecord, Boolean> pair = record.undo(transactionEntry.lastLSN);
                    LogRecord CLR = pair.getFirst();
                    Boolean flushed = pair.getSecond();
                    prevLSN = this.logManager.appendToLog(CLR);
                    transactionEntry.lastLSN = prevLSN;

                    /*if(CLR.getPageNum().isPresent()){
                        CLRPageNum = CLR.getPageNum();
                    }*/
                    if(CLR instanceof UndoUpdatePageLogRecord){
                        this.dirtyPageTable.putIfAbsent(CLR.getPageNum().get(), transactionEntry.lastLSN);
                        System.out.println(prevLSN);
                    }else if(CLR instanceof UndoAllocPageLogRecord){
                        this.dirtyPageTable.remove(transactionEntry.lastLSN);
                    }

                    CLR.redo(this.diskSpaceManager, this.bufferManager);

                    if (flushed && !(CLR instanceof UndoUpdatePageLogRecord)){
                        this.logManager.flushToLSN(prevLSN);
                    }
                }

                if(record.getPrevLSN().isPresent()){
                    prevLSN = record.getPrevLSN().get();
                    record = this.logManager.fetchLogRecord(prevLSN);
                }else{
                   //this.dirtyPageTable.put(CLRPageNum.get(), transactionEntry.lastLSN);
                   break;
                }
            }
        }

        LogRecord record = new EndTransactionLogRecord(transNum, transactionEntry.lastLSN);

        //the transaction should be removed from the transaction table
        this.transactionTable.remove(transNum);

        //An end record should be emitted
        long LSN  = logManager.appendToLog(record);

        //the transaction status should be updated
        transactionEntry.lastLSN = LSN;
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);

        return LSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long prevLSN = transactionEntry.lastLSN;

        //The appropriate log record should be emitted
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);

        //The appropriate log record should be emitted
        if (record.toBytes().length > BufferManager.EFFECTIVE_PAGE_SIZE / 2){
            LogRecord undoOnly = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, null);
            prevLSN = this.logManager.appendToLog(undoOnly);
            //this.logManager.flushToLSN(prevLSN);

            LogRecord redoOnly = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, null, after);
            long LSN = this.logManager.appendToLog(redoOnly);
            //this.logManager.flushToLSN(LSN);

            //Both the transaction table and dirty page table should be updated accordingly.
            transactionEntry.lastLSN = LSN;
            transactionEntry.touchedPages.add(pageNum);
            this.dirtyPageTable.putIfAbsent(pageNum, LSN);

            return LSN;
        }else{
            long LSN = this.logManager.appendToLog(record);
            //this.logManager.flushToLSN(LSN);

            //Both the transaction table and dirty page table should be updated accordingly.
            transactionEntry.lastLSN = LSN;
            transactionEntry.touchedPages.add(pageNum);
            this.dirtyPageTable.putIfAbsent(pageNum, LSN);

            return LSN;
        }
        //return -1L;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        //All changes done by the transaction since the savepoint should be undone
        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = this.logManager.fetchLogRecord(prevLSN);

        while (prevLSN > LSN){
            if (record != null && record.isUndoable()){
                Pair<LogRecord, Boolean> pair = record.undo(transactionEntry.lastLSN);
                LogRecord CLR = pair.getFirst();
                Boolean flushed = pair.getSecond();
                prevLSN = this.logManager.appendToLog(CLR);
                transactionEntry.lastLSN = prevLSN;

                if(CLR instanceof UndoUpdatePageLogRecord){
                    this.dirtyPageTable.putIfAbsent(CLR.getPageNum().get(), transactionEntry.lastLSN);
                }else if(CLR instanceof UndoAllocPageLogRecord){
                    //System.out.println(CLR.getPageNum().get());
                    this.dirtyPageTable.remove(CLR.getPageNum().get());
                }

                if (flushed && !(CLR instanceof UndoUpdatePageLogRecord)){
                    this.logManager.flushToLSN(prevLSN);
                }

                CLR.redo(this.diskSpaceManager, this.bufferManager);
            }

            if (record.getUndoNextLSN().isPresent()){
                prevLSN = record.getUndoNextLSN().get();

                if (prevLSN > LSN){
                    record = this.logManager.fetchLogRecord(prevLSN);
                }else{
                    break;
                }
            }else if(record.getPrevLSN().isPresent()){
                prevLSN = record.getPrevLSN().get();

                if (prevLSN > LSN){
                    record = this.logManager.fetchLogRecord(prevLSN);
                }else{
                    break;
                }
                //record = this.logManager.fetchLogRecord(prevLSN);
            }else{
                //this.dirtyPageTable.put(CLRPageNum.get(), transactionEntry.lastLSN);
                break;
            }
        }
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (Map.Entry<Long, Long> entry : this.dirtyPageTable.entrySet()){
            long pageNum = entry.getKey();
            long recLSN = entry.getValue();

            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                        dpt.size() + 1, txnTable.size(), touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            dpt.put(pageNum, recLSN);
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()){
            long transNum = entry.getKey();
            TransactionTableEntry transactionTableEntry = entry.getValue();
            Transaction.Status status = transactionTableEntry.transaction.getStatus();
            long lastLSN = transactionTableEntry.lastLSN;

            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(), txnTable.size() + 1, touchedPages.size(), numTouchedPages);

            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            txnTable.put(transNum,new Pair<>(status, lastLSN));
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(proj5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(proj5): implement
        restartAnalysis();
        restartRedo();

        List<Long> dirtyPageNums = new ArrayList<>();
        this.bufferManager.iterPageNums((pageNum, dirty) -> {
            if(dirty){
                dirtyPageNums.add(pageNum);
            }
        });

        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){
            if(!dirtyPageNums.contains(entry.getKey())){
                dirtyPageTable.remove(entry.getKey(), entry.getValue());
            }
        }

        Runnable run = new Runnable(){
            public void run(){
                restartUndo();
                checkpoint();
            }
        };

        return run;
        //return () -> {};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(proj5): implement
        //We then begin scanning log records, starting at the begin checkpoint record.
        Iterator<LogRecord> iter = this.logManager.scanFrom(LSN);
        while(iter.hasNext()){
            record = iter.next();

            //If the log record is for a transaction operation:
            if (record.getTransNum().isPresent()){
                long transNum = record.getTransNum().get();

                // - update the transaction table
                if (this.transactionTable.isEmpty() || !this.transactionTable.containsKey(transNum)){
                    Transaction transaction = newTransaction.apply(transNum);
                    this.transactionTable.putIfAbsent(transNum, new TransactionTableEntry(transaction));
                }
                this.transactionTable.get(transNum).lastLSN = record.getLSN();

                // - if it's page-related (as opposed to partition-related),
                if (record.getPageNum().isPresent()){
                    long pageNum = record.getPageNum().get();

                    // - add to touchedPages
                    this.transactionTable.get(transNum).touchedPages.add(pageNum);

                    // - acquire X lock
                    acquireTransactionLock(this.transactionTable.get(transNum).transaction,
                            getPageLockContext(pageNum), LockType.X);

                    // - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
                    if(record instanceof FreePageLogRecord || record instanceof UndoAllocPageLogRecord){
                        this.dirtyPageTable.remove(pageNum);
                    }else if(record instanceof UndoUpdatePageLogRecord || record instanceof UpdatePageLogRecord){
                        this.dirtyPageTable.putIfAbsent(pageNum, this.transactionTable.get(transNum).lastLSN);
                    }
                }
            }

            //If the log record is for a change in transaction status:
            if(record.getTransNum().isPresent() && (record instanceof CommitTransactionLogRecord
                    || record instanceof AbortTransactionLogRecord || record instanceof EndTransactionLogRecord)){
                long transNum = record.getTransNum().get();

                // - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
                if(record instanceof CommitTransactionLogRecord){
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
                }else if (record instanceof AbortTransactionLogRecord){
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                }else{
                    // - clean up transaction (Transaction#cleanup) if END_TRANSACTION
                    for(Map.Entry<Long, Pair<Transaction.Status, Long>> entry : record.getTransactionTable().entrySet()){
                        this.transactionTable.get(entry.getKey()).transaction.cleanup();
                    }
                    //this.transactionTable.get(transNum).transaction.cleanup();
                    this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                    // - update the transaction table?
                    this.transactionTable.remove(transNum);
                }
            }

            //If the log record is a begin_checkpoint record:
            if (record instanceof BeginCheckpointLogRecord){
                // - Update the transaction counter
                this.updateTransactionCounter.accept(Math.max(this.getTransactionCounter.get(),
                        record.getMaxTransactionNum().get()));
            }

            //If the log record is an end_checkpoint record:
            if (record instanceof EndCheckpointLogRecord){
                // - Copy all entries of checkpoint DPT (replace existing entries if any)
                //LogRecord checkPoint = this.logManager.fetchLogRecord(LSN);
                for (Map.Entry<Long, Long> entry : record.getDirtyPageTable().entrySet()){
                    this.dirtyPageTable.put(entry.getKey(), entry.getValue());
                }

                // - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry: record.getTransactionTable().entrySet()){
                    if (entry.getValue().getSecond() <= LSN){
                        entry.setValue(new Pair<>(entry.getValue().getFirst(), LSN));

                    }

                    //   add to transaction table if not already present.
                    if (!this.transactionTable.containsKey(entry.getKey())){
                        Transaction transaction = newTransaction.apply(entry.getKey());
                        startTransaction(transaction);
                        if(transaction.getStatus() == Transaction.Status.COMPLETE){
                            this.transactionTable.remove(entry.getKey());
                        }else{
                            this.transactionTable.putIfAbsent(entry.getKey(),
                                    new TransactionTableEntry(transaction));
                        }

                    }
                }

                // - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
                //   transaction table if the transaction has not finished yet, and acquire X locks.
                for (Map.Entry<Long, List<Long>> entry: record.getTransactionTouchedPages().entrySet()){
                    if(this.transactionTable.containsKey(entry.getKey())
                            && this.transactionTable.get(entry.getKey()).transaction.getStatus() != Transaction.Status.COMPLETE){
                        for (Long pageNum : entry.getValue()){
                            this.transactionTable.get(entry.getKey()).touchedPages.add(pageNum);
                            acquireTransactionLock(this.transactionTable.get(entry.getKey()).transaction,
                                    getPageLockContext(pageNum), LockType.X);
                        }
                    }
                }
            }

            //Then, cleanup and end transactions that are in the COMMITING state, and
            //move all transactions in the RUNNING state to RECOVERY_ABORTING.
            for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()){
                if (entry.getValue().transaction.getStatus() == Transaction.Status.COMMITTING) {
                    entry.getValue().transaction.cleanup();
                    entry.getValue().transaction.setStatus(Transaction.Status.COMPLETE);

                    LogRecord endRecord = new EndTransactionLogRecord(entry.getKey(),entry.getValue().lastLSN);
                    Long newLSN = this.logManager.appendToLog(endRecord);
                    entry.getValue().lastLSN = newLSN;

                    this.transactionTable.remove(entry.getKey());
                }else if(entry.getValue().transaction.getStatus() == Transaction.Status.RUNNING){
                    entry.getValue().transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);

                    LogRecord abortRecord = new AbortTransactionLogRecord(entry.getKey(),entry.getValue().lastLSN);
                    Long newLSN = this.logManager.appendToLog(abortRecord);
                    entry.getValue().lastLSN = newLSN;
                }
            }
        }

        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(proj5): implement
        //First, determine the starting point for REDO from the DPT.
        Long lowestRecLSN = Long.MAX_VALUE;
        //Long startPoint = -1l;
        for (Map.Entry<Long, Long> entry : this.dirtyPageTable.entrySet()){
            if(entry.getValue() < lowestRecLSN){
                lowestRecLSN = entry.getValue();
                //startPoint = entry.getKey();
            }
        }

        //Then, scanning from the starting point, if the record is redoable and
        // - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
        //   the page is fetched from disk and the pageLSN is checked, and the record is redone.
        // - about a partition (Alloc/Free/Undo..Part), redo it.
        Iterator<LogRecord>  iter = this.logManager.scanFrom(lowestRecLSN);

        while(iter.hasNext()){
            LogRecord record = iter.next();

            if(record.isRedoable() && (record.getPartNum().isPresent()
                    || (record.getPageNum().isPresent()
                    && this.dirtyPageTable.containsKey(record.getPageNum().get())
                    && (record.getLSN() >= this.dirtyPageTable.get(record.getPageNum().get()))
                    && (record.getLSN() > this.bufferManager.fetchPage(getPageLockContext(record.getPageNum().get()),
                    record.getPageNum().get(), true).getPageLSN())  ))){
                record.redo(diskSpaceManager, bufferManager);
            }
        }

        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement

        //First, a priority queue is created sorted on lastLSN of all aborting transactions.
        PriorityQueue<Pair<Long, Long>> pq = new PriorityQueue<>(new PairFirstReverseComparator());

        for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()){
            if (entry.getValue().transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING){
                pq.add(new Pair<>(entry.getValue().lastLSN, entry.getKey()));
            }
        }

        //Then, always working on the largest LSN in the priority queue until we are done,
        // - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
        // - replace the entry in the set should be replaced with a new one, using the undoNextLSN
        //   (or prevLSN if none) of the record; and
        // - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
        while (!pq.isEmpty()){
            Pair<Long,Long> pqPair = pq.poll();
            Long prevLSN = pqPair.getFirst();
            Long transNum = pqPair.getSecond();
            LogRecord record = this.logManager.fetchLogRecord(prevLSN);
            System.out.println(record);

            //if the record is undoable, we undo it*
            if(record.isUndoable()){
                Pair<LogRecord, Boolean> pair = record.undo(prevLSN);
                LogRecord CLR = pair.getFirst();
                Boolean flushed = pair.getSecond();
                //write the CLR out
                prevLSN = this.logManager.appendToLog(CLR);
                this.transactionTable.get(transNum).lastLSN = prevLSN;

                //updating the DPT as necessary
                if(CLR instanceof UndoUpdatePageLogRecord){
                    this.dirtyPageTable.putIfAbsent(CLR.getPageNum().get(), prevLSN);
                }else if(CLR instanceof UndoAllocPageLogRecord){
                    //System.out.println(CLR.getPageNum().get());
                    this.dirtyPageTable.remove(CLR.getPageNum().get());
                }

                if (flushed && !(CLR instanceof UndoUpdatePageLogRecord)){
                    this.logManager.flushToLSN(prevLSN);
                }

                CLR.redo(this.diskSpaceManager, this.bufferManager);
            }

            //replace the LSN in the set with the undoNextLSN of the record if it has one, and the prevLSN otherwise;
            if (record.getUndoNextLSN().isPresent()){
                prevLSN = record.getUndoNextLSN().get();
                pq.add(new Pair<>(prevLSN, transNum));
            }else if (record.getPrevLSN().isPresent()){
                prevLSN = record.getPrevLSN().get();
                pq.add(new Pair<>(prevLSN, transNum));
            }

            //end the transaction if the LSN from the previous step is 0, removing it from the set and the transaction table.
            if (prevLSN == 0){
                EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transNum, prevLSN);
                long LSN  = logManager.appendToLog(endRecord);
                //this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);

                pq.remove(prevLSN);
                this.transactionTable.remove(transNum);

            }

        }


        return;
    }

    // TODO(proj5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
