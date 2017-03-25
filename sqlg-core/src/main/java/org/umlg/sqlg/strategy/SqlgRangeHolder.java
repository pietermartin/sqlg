package org.umlg.sqlg.strategy;

import org.apache.commons.lang3.Range;

/**
 * Date: 2017/03/25
 * Time: 2:42 PM
 */
public class SqlgRangeHolder {

    private Range<Long> range;
    private boolean applyOnDb = true;

    private SqlgRangeHolder(Range<Long> range) {
        this.range = range;
    }

    public Range<Long> getRange() {
        return range;
    }

    public void doNotApplyOnDb() {
        this.applyOnDb = false;
    }

    public boolean isApplyOnDb() {
        return applyOnDb;
    }

    public static SqlgRangeHolder from(Range<Long> range) {
        return new SqlgRangeHolder(range);
    }

    public boolean hasRange() {
        return this.range != null;
    }
}
