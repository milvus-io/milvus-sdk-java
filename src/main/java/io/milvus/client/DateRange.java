package io.milvus.client;

import javax.annotation.Nonnull;
import java.util.Date;

public class DateRange {
    private Date startDate;
    private Date endDate;

    public DateRange(@Nonnull Date startDate, @Nonnull Date endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public void setStartDate(@Nonnull Date startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(@Nonnull Date endDate) {
        this.endDate = endDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    @Override
    public String toString() {
        return "{startDate=" + startDate +
                ", endDate=" + endDate +
                '}';
    }
}
