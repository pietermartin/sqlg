package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.Range;

/**
 * Date: 2017/03/25
 * Time: 2:42 PM
 */
public class SqlgRangeHolder {

    private long skip = -1;
    private Range<Long> range;
    private boolean applyOnDb = true;
    private boolean applyInStep = true;

    private SqlgRangeHolder(Range<Long> range) {
        this.range = range;
    }

    private SqlgRangeHolder(long skip) {
        this.skip = skip;
    }

    public static SqlgRangeHolder from(Range<Long> range) {
        return new SqlgRangeHolder(range);
    }

    public static SqlgRangeHolder from(long skip) {
        return new SqlgRangeHolder(skip);
    }

    public Range<Long> getRange() {
        return range;
    }

    public void doNotApplyOnDb() {
        this.applyOnDb = false;
    }

    public void doNotApplyInStep() {
        this.applyInStep = false;
    }

    public void reset() {
        this.applyOnDb = true;
        this.applyInStep = true;
    }

    public boolean isApplyOnDb() {
        return applyOnDb;
    }


    public boolean hasRange() {
        return this.range != null;
    }

    public boolean isApplyInStep() {
        return applyInStep;
    }

    public void setApplyInStep(boolean applyInStep) {
        this.applyInStep = applyInStep;
    }

    public boolean hasSkip() {
        return this.skip != -1;
    }

    public long getSkip() {
        return skip;
    }
}
