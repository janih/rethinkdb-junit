package com.foobarsite.rss.feed;

import java.util.Date;

public interface IFeedItem extends IEntity, Comparable<IFeedItem> {

	String getFeedId();
	String getLink();
	String getTitle();
	String getContent();
	String getAuthor();
	Date getPublishDate();
	Date getCreatedDate();
	Date getUpdatedDate();
	String getUri();
	boolean isRead();
	void setRead(boolean read);
	boolean isLiked();
	void setLiked(boolean liked);
}