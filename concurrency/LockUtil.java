package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement
        //System.out.println("LockUtil!");

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        if (transaction == null || lockType == LockType.NL || lockContext.readonly == true){
            return;
        }

        List<LockContext> lst = new ArrayList<>();

        LockContext currentContext = lockContext;
        while (currentContext != null){
            lst.add(currentContext);
            currentContext = currentContext.parentContext();
        }

        while (!lst.isEmpty()){
            currentContext = lst.remove(lst.size()-1);

            LockType type = currentContext != lockContext ? LockType.parentLock(lockType) : lockType;
            LockType currentType = currentContext.getExplicitLockType(transaction);

            /*if (currentContext.parentContext() != null
                    && currentContext.parentContext().getResourceName().getCurrentName().getFirst().compareTo("database") != 0
                    && currentContext.capacity() == 0
                    && currentContext.parentContext().capacity() >= 10
                    && currentContext.parentContext().saturation(TransactionContext.getTransaction()) >= 0.2
                    && currentContext.parentContext().enableAutoEscalated == true){
                currentContext.parentContext().escalate(transaction);
            }*/

            if (currentType == LockType.NL){
                currentContext.acquire(transaction, type);

            }else if (currentType == LockType.S){
                if (type == LockType.S || type == LockType.IS){
                    return;
                }else if (type == LockType.X){
                    currentContext.promote(transaction, type);
                }else if (type == LockType.IX){
                    currentContext.promote(transaction, LockType.SIX);
                }

            }else if (currentType == LockType.IS){
                if (type == LockType.S || type == LockType.X){
                    currentContext.escalate(transaction);
                }else if (type == LockType.IX){
                    currentContext.promote(transaction, type);
                }

            }else if (currentType == LockType.X){
                return;

            }else if (currentType == LockType.IX){
                if (type == LockType.S){
                    currentContext.promote(transaction, LockType.SIX);
                }else if (type == LockType.X){
                    currentContext.escalate(transaction);
                }

            }else if (currentType == LockType.SIX){
                if (type == LockType.S){
                    return;
                }else if (type == LockType.X){
                    currentContext.escalate(transaction);
                }

            }

        }

    }


    // TODO(proj4_part2): add helper methods as you see fit
}
