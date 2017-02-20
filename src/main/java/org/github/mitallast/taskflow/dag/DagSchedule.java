package org.github.mitallast.taskflow.dag;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DagSchedule {
    private final String token;
    private final boolean enabled;
    private final String cronExpression;

    @JsonCreator
    public DagSchedule(
        @JsonProperty("token") String token,
        @JsonProperty("enabled") boolean enabled,
        @JsonProperty("cronExpression") String cronExpression
    ) {
        this.token = token;
        this.enabled = enabled;
        this.cronExpression = cronExpression;
    }

    public String token() {
        return token;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String cronExpression() {
        return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DagSchedule that = (DagSchedule) o;

        if (enabled != that.enabled) return false;
        if (!token.equals(that.token)) return false;
        return cronExpression.equals(that.cronExpression);
    }

    @Override
    public int hashCode() {
        int result = token.hashCode();
        result = 31 * result + (enabled ? 1 : 0);
        result = 31 * result + cronExpression.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DagSchedule{" +
            "token='" + token + '\'' +
            ", enabled=" + enabled +
            ", cronExpression='" + cronExpression + '\'' +
            '}';
    }
}
