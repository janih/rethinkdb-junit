package com.foobarsite.rss.feed;

import java.util.Date;
import java.util.Set;

public interface IFeed extends IEntity, Comparable<IFeed> {

	String getName();
	void setName(String name);
	String getUrl();
	String getDescription();
	void setDescription(String description);
	String getAuthor();
	void setAuthor(String author);
	String getUri();
	void setUri(String uri);
	String getCopyright();
	void setCopyright(String copyright);
	String getLanguage();
	void setLanguage(String language);
	String getImageUrl();
	void setImageUrl(String imageUrl);
	String getCategories();
	void setCategories(String categories);
	Set<IFeedItem> getFeedItems();
	Set<IFeedItem> getNewFeedItems();
	void clearItems();
	Date getLastOkFetch();
	void setLastOkFetch(Date date);
	Date getLastFailedFetch();
	void setLastFailedFetch(Date date);
	String getLastFailMsg();
	void setLastFailMsg(String msg);
	String getLastFailResponseCode();
	void setLastFailResponseCode(String lastFailResponseCode);
	Date getUpdated();
	void setUpdated(Date updated);
}
