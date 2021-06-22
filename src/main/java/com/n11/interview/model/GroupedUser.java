package com.n11.interview.model;

public class GroupedUser {
    private String userId;
    private Long count;
    private Long started;
    private Long ended;

    public GroupedUser() {
    }

    public GroupedUser(String userId, Long count) {
        this.userId = userId;
        this.count = count;
    }

    public GroupedUser(String userId, Long count, Long started, Long ended) {
        this.userId = userId;
        this.count = count;
        this.started = started;
        this.ended = ended;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getStarted() {
        return started;
    }

    public void setStarted(Long started) {
        this.started = started;
    }

    public Long getEnded() {
        return ended;
    }

    public void setEnded(Long ended) {
        this.ended = ended;
    }

   @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupedUser groupedUser = (GroupedUser) o;
        return userId.equals(groupedUser.userId) && started.equals(groupedUser.started) && ended.equals(groupedUser.ended);
    }

    @Override
    public int hashCode() {
        return (int)(started + ended + userId.hashCode());
    }
}
