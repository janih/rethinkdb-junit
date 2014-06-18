package com.foobarsite.rss.feed.desc;


public enum UserFeedProperty implements ITable {
	TABLE_NAME("userblog"),
	GEN_ID("id"),
	FEED_ID("feed_id"),
	USER_ID("userid"),
	GROUP("group"),
	READ_IDS("read_ids"),
	LIKED_IDS("liked_ids"),
	CREATED("created"),
	UPDATED("updated");

	private final String property;

	private UserFeedProperty(String property) {
		this.property = property;
	}

	@Override
	public String toString() {
		return property;
	}

	@Override
	public String getTableName() {
		return TABLE_NAME.toString();
	}

	@Override
	public UserFeedProperty getId() {
		return GEN_ID;
	}

}
