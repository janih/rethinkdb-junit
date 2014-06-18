package com.foobarsite.rss.server;

import java.util.List;

import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.IFeedItem;
import com.foobarsite.rss.feed.Pair;
import com.foobarsite.rss.feed.desc.ITable;

public interface IFeedReader {

	String getDbName();
	int getRowCountFor(ITable table);
	Pair<Boolean, Boolean> canUpdate(final String feedId);
	void setFetchInterval(int days, int hours, int minutes);

	IFeed save(IFeed feed);
	IFeed loadFeed(String url);
	IFeed loadFeedItems(IFeed feed);
	List<IFeed> loadFeeds(List<String> ids, boolean loadContent);

	boolean deleteItem(IFeedItem feed);
	IFeedItem getExistingFeedItem(final String feedid, String link, String uri, String title);
	List<IFeedItem> saveItems(IFeed feed);
	boolean saveItem(IFeedItem item);

	List<String> getProperties(ITable property);

	int updateFeeds(List<String> ids);
	int trimFeedUrlSlashes(List<String> ids);
	int trimItemWhiteSpace(List<String> ids);

	void testFeedItems();
	void testFeeds();

}