package com.foobarsite.rss.feed.desc;


public enum FeedProperty implements ITable {
	TABLE_NAME("blog"),
	GEN_ID("id"),
	NAME("name"),
	URL("url"),
	URI("uri"),
	DESCRIPTION("description"),
	AUTHOR("author"),
	COPYRIGHT("copyright"),
	LANGUAGE("lang"),
	IMAGE_URL("imageurl"),
	CATEGORIES("categories"),
	LAST_FAILED_FETCH("lastfailedfetch"),
	LAST_FAIL_MSG("lastfailmsg"),
	LAST_FAIL_RESPONSE("lastfailresp"),
	LAST_OK_FETCH("lastokfetch"),
	CREATED("created"),
	UPDATED("updated");

	private final String property;

	private FeedProperty(String property) {
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
	public FeedProperty getId() {
		return GEN_ID;
	}

}
