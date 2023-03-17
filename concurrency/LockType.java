package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if ((a == X && b != NL) || (b == X && a != NL)) {
            return false;
        }else if ((a == SIX && ((b != NL) && (b != IS))) || (b == SIX && ((a != NL) && (a != IS)))) {
            return false;
        }else if ((a == S && b == IX) || (b == S && a == IX)) {
            return false;
        }else{
            return true;
        }
        //return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (parentLockType == NL && childLockType != NL){
            return false;
        }

        switch (childLockType) {
            case S:
                if (parentLockType == IS || parentLockType == IX) {
                    return true;
                }
                break;
            case IS:
                if (parentLockType == IS || parentLockType == IX) {
                    return true;
                }
                break;
            case X:
                if (parentLockType == IX || parentLockType == SIX) {
                    return true;
                }
                break;
            case IX:
                if (parentLockType == IX || parentLockType == SIX) {
                    return true;
                }
                break;
            case SIX:
                if (parentLockType == IX) {
                    return true;
                }
                break;
            case NL:
                return true;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (required) {
            case S:
                if (substitute == S || substitute == X || substitute == SIX) {
                    return true;
                }
                break;
            case X:
                if (substitute == X) {
                    return true;
                }
                break;
            case IS:
                if (substitute == IS || substitute == IX || substitute == S || substitute == X) {
                    return true;
                }
                break;
            case IX:
                if (substitute == IX || substitute == X) {
                    return true;
                }
                break;
            case SIX:
                if (substitute == SIX || substitute == X) {
                    return true;
                }
                break;
            case NL:
                return true;
        }

        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

