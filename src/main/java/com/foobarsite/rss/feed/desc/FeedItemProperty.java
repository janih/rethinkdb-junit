package com.foobarsite.rss.feed.desc;


public enum FeedItemProperty implements ITable {
	TABLE_NAME("blog_item"),
	GEN_ID("id"),
	FEED_ID("feed_id"),
	LINK("link"),
	URI("uri"),
	TITLE("title"),
	CONTENT("content"),
	AUTHOR("author"),
	PUBLISHED("published"),
	CREATED("created"),
	UPDATED("updated");

	private final String property;
	private FeedItemProperty(String property) {
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
	public FeedItemProperty getId() {
		return GEN_ID;
	}

}
