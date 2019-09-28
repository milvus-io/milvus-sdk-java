package io.milvus.client.params;

import javax.annotation.Nonnull;

public class DeleteByRangeParam {
    private final DateRange dateRange;
    private final String tableName;

    public static final class Builder {
        // Required parameters
        private final DateRange dateRange;
        private final String tableName;

        public Builder(DateRange dateRange, String tableName) {
            this.dateRange = dateRange;
            this.tableName = tableName;
        }

        public DeleteByRangeParam build() {
            return new DeleteByRangeParam(this);
        }
    }

    private DeleteByRangeParam(@Nonnull Builder builder) {
        this.dateRange = builder.dateRange;
        this.tableName = builder.tableName;
    }

    public DateRange getDateRange() {
        return dateRange;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "DeleteByRangeParam {" +
                "dateRange=" + dateRange +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
