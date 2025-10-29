package io.milvus.v2.service.utility.response;

import java.util.ArrayList;
import java.util.List;

public class CheckHealthResp {
    private Boolean isHealthy;
    private List<String> reasons;
    private List<String> quotaStates;

    private CheckHealthResp(CheckHealthRespBuilder builder) {
        this.isHealthy = builder.isHealthy;
        this.reasons = builder.reasons;
        this.quotaStates = builder.quotaStates;
    }

    public static CheckHealthRespBuilder builder() {
        return new CheckHealthRespBuilder();
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
    public String toString() {
        return "CheckHealthResp{" +
                "isHealthy=" + isHealthy +
                ", reasons=" + reasons +
                ", quotaStates=" + quotaStates +
                '}';
    }

    public static class CheckHealthRespBuilder {
        private Boolean isHealthy = false;
        private List<String> reasons = new ArrayList<>();
        private List<String> quotaStates = new ArrayList<>();

        public CheckHealthRespBuilder isHealthy(Boolean isHealthy) {
            this.isHealthy = isHealthy;
            return this;
        }

        public CheckHealthRespBuilder reasons(List<String> reasons) {
            this.reasons = reasons;
            return this;
        }

        public CheckHealthRespBuilder quotaStates(List<String> quotaStates) {
            this.quotaStates = quotaStates;
            return this;
        }

        public CheckHealthResp build() {
            return new CheckHealthResp(this);
        }
    }
}
