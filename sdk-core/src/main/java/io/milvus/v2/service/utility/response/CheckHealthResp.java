package io.milvus.v2.service.utility.response;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

public class CheckHealthResp {
    private Boolean isHealthy;
    private List<String> reasons;
    private List<String> quotaStates;

    private CheckHealthResp(Builder builder) {
        this.isHealthy = builder.isHealthy;
        this.reasons = builder.reasons;
        this.quotaStates = builder.quotaStates;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Boolean getIsHealthy() {
        return isHealthy;
    }

    public void setIsHealthy(Boolean isHealthy) {
        this.isHealthy = isHealthy;
    }

    public List<String> getReasons() {
        return reasons;
    }

    public void setReasons(List<String> reasons) {
        this.reasons = reasons;
    }

    public List<String> getQuotaStates() {
        return quotaStates;
    }

    public void setQuotaStates(List<String> quotaStates) {
        this.quotaStates = quotaStates;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CheckHealthResp that = (CheckHealthResp) obj;
        return new EqualsBuilder()
                .append(isHealthy, that.isHealthy)
                .append(reasons, that.reasons)
                .append(quotaStates, that.quotaStates)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(isHealthy)
                .append(reasons)
                .append(quotaStates)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "CheckHealthResp{" +
                "isHealthy=" + isHealthy +
                ", reasons=" + reasons +
                ", quotaStates=" + quotaStates +
                '}';
    }

    public static class Builder {
        private Boolean isHealthy = false;
        private List<String> reasons = new ArrayList<>();
        private List<String> quotaStates = new ArrayList<>();

        public Builder isHealthy(Boolean isHealthy) {
            this.isHealthy = isHealthy;
            return this;
        }

        public Builder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public Builder quotaStates(List<String> quotaStates) {
            this.quotaStates = quotaStates;
            return this;
        }

        public CheckHealthResp build() {
            return new CheckHealthResp(this);
        }
    }
}
