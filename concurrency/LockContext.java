package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    //Part 3 HERE
    public boolean enableAutoEscalated;
    //Part 3 END

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
        this.enableAutoEscalated = true;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("Context is readonly");
        }

        /*if (parent != null && !LockType.substitutable(parent.getExplicitLockType(transaction), LockType.parentLock(lockType))){
            throw new InvalidLockException("The request is invalid");
        }*/

        if (parent != null && (parent.getExplicitLockType(transaction) != LockType.NL
                && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType))){
            throw new InvalidLockException("The request is invalid");
        }

        if (hasSIXAncestor(transaction) && (lockType == LockType.S || lockType == LockType.IS)){
            throw new InvalidLockException("The request is invalid");
        }

        if (getExplicitLockType(transaction) == lockType){
            throw new DuplicateLockRequestException("A lock is already held by TRANSACTION");
        }

        lockman.acquire(transaction, name, lockType);
        if(parent != null){
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
            //System.out.println(parent.numChildLocks.getOrDefault(name.getCurrentName().getSecond(), 0));
        }/*else{
            numChildLocks.put(transaction.getTransNum(), 0);
        }*/

        return;
    }


    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //System.out.println("LockContext.release");
        if (readonly){
            throw new UnsupportedOperationException("Context is readonly");
        }

        if (lockman.getLockType(transaction, name) == LockType.NL){
            throw new NoLockHeldException("No lock on NAME is held by TRANSACTION");
        }

        /*LockContext child = childContext(numChildLocks.get(transaction.getTransNum()));
        System.out.println(child.getEffectiveLockType(transaction));

        if (numChildLocks.keySet().contains(transaction.getTransNum())
                && numChildLocks.get(transaction.getTransNum()) != 0
                && !LockType.canBeParentLock(LockType.NL, child.getExplicitLockType(transaction))){
            throw new InvalidLockException("The lock cannot be released");
        }*/

        if (numChildLocks.keySet().contains(transaction.getTransNum()) && numChildLocks.get(transaction.getTransNum()) != 0){
            LockContext child = childContext(numChildLocks.get(transaction.getTransNum()));
            if (!LockType.canBeParentLock(LockType.NL, child.getExplicitLockType(transaction))){
                throw new InvalidLockException("The lock cannot be released");
            }
        }


        lockman.release(transaction, name);
        if (parent != null){
            parent.numChildLocks.put(transaction.getTransNum(),
                    parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) - 1);
        }
        /*else{
            numChildLocks.put(transaction.getTransNum(),
                    numChildLocks.getOrDefault(transaction.getTransNum(), 0) - 1);
        }*/

        return;
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("Context is readonly");
        }

        if (lockman.getLockType(transaction, name) == newLockType){
            throw new DuplicateLockRequestException("TRANSACTION already has a NEWLOCKTYPE lock");
        }

        if (lockman.getLockType(transaction, name) == LockType.NL){
            throw new NoLockHeldException("TRANSACTION has no lock");
        }

        //System.out.println(newLockType);
        //System.out.println(getEffectiveLockType(transaction));
        //System.out.println(getExplicitLockType(transaction));
        //System.out.println(name);
        if (!(LockType.substitutable(newLockType, getExplicitLockType(transaction))
                && newLockType != getExplicitLockType(transaction)) && newLockType != LockType.SIX){
            throw new InvalidLockException("The requested lock type is not a promotion or promoting" +
                    "would cause the lock manager to enter an invalid state ");
        }

        if (hasSIXAncestor(transaction) && newLockType == LockType.SIX){
            return;
        }else if(newLockType == LockType.SIX){
            List<ResourceName> lst = sisDescendants(transaction);
            //System.out.println(lockman.getLocks(transaction));
            //System.out.println(lst);
            lst.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, lst);
        }else{
            lockman.promote(transaction, name, newLockType);
            if(lockman.getLockType(transaction, name) == LockType.NL){
                parent.numChildLocks.put(transaction.getTransNum(),
                        parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
            }

        }

        return;
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly){
            throw new UnsupportedOperationException("Context is readonly");
        }

        if (getExplicitLockType(transaction) == LockType.NL){
            throw new NoLockHeldException("TRANSACTION has no lock at this level");
        }

        List<ResourceName> lst = new ArrayList<>();
        //LockContext child = childContext(transaction.getTransNum());
        List<Lock> lstOfLocks = lockman.getLocks(transaction);

        if (lstOfLocks.size() == 1){
            if (getExplicitLockType(transaction) == LockType.IS){
                lst.add(name);
                promote(transaction, LockType.S);
            }else if (getExplicitLockType(transaction) == LockType.IX){
                lst.add(name);
                promote(transaction, LockType.X);
            }
        }else{
            LockType newLockType = LockType.S;
            lstOfLocks.remove(0);
            for (Lock lock : lstOfLocks){
                if (lock.lockType == LockType.X){
                    newLockType = LockType.X;
                }
                lst.add(lock.name);
            }

            if (getExplicitLockType(transaction) == LockType.IX){
                newLockType = LockType.X;
            }

            //System.out.println(lst);
            //System.out.println(newLockType);

            lockman.acquireAndRelease(transaction, name, newLockType, lst);
            //System.out.println(lockman.getLocks(transaction));

            if (parent != null){
                parent.numChildLocks.put(transaction.getTransNum(),
                        parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0) -
                                lstOfLocks.size());
            }else{
                numChildLocks.put(transaction.getTransNum(),
                        numChildLocks.getOrDefault(transaction.getTransNum(), 0) -
                                lstOfLocks.size());
            }

            //System.out.println(lockman.getLocks(transaction));
           // System.out.println(numChildLocks.getOrDefault(transaction.getTransNum(), 0));
            /*numChildLocks.put(transaction.getTransNum(),
                    numChildLocks.getOrDefault(transaction.getTransNum(), 0) -
                            child.lockman.getLocks(child.name).size());*/
        }

        return;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType type = getExplicitLockType(transaction);

        if (parent == null && lockman.getLocks(transaction).size() == 1 && type == LockType.SIX){
            return LockType.S;
        }

        if (type == LockType.NL) {
            if (parent != null) {

                type = parent.getEffectiveLockType(transaction);

                if (type == LockType.IS || type == LockType.IX) {
                    return LockType.NL;
                }else if (type == LockType.SIX){
                    return LockType.S;
                }
            }
        }

        return type;

        //return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parent != null && parent.getExplicitLockType(transaction) == LockType.SIX){
            return true;
        }

        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> lst = lockman.getLocks(transaction);
        List<ResourceName> newLst = new ArrayList<>();

        for (Lock lock : lst){
            if ((lock.lockType == LockType.IS || lock.lockType == LockType.S) && lock.name.isDescendantOf(name)){
                newLst.add(lock.name);
            }
        }

        return newLst;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

