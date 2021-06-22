package com.n11.interview.model;

public class JoinedUsers {
    private GroupedUser topUser;
    private GroupedUser user;

    public JoinedUsers() {
    }

    public JoinedUsers(GroupedUser topUser, GroupedUser user) {
        this.topUser = topUser;
        this.user = user;
    }

    public GroupedUser getTopUser() {
        return topUser;
    }

    public void setTopUser(GroupedUser topUser) {
        this.topUser = topUser;
    }

    public GroupedUser getUser() {
        return user;
    }

    public void setUser(GroupedUser user) {
        this.user = user;
    }
}
