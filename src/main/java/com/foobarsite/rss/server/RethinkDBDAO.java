package com.foobarsite.rss.server;

import static com.foobarsite.rss.common.DateTools.DEFAULT_DATE_MAX;
import static com.foobarsite.rss.common.DateTools.DEFAULT_DATE_MIN;
import static com.foobarsite.rss.common.StringTools.areEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dkhenry.RethinkDB.Function;
import com.dkhenry.RethinkDB.RqlConnection;
import com.dkhenry.RethinkDB.RqlCursor;
import com.dkhenry.RethinkDB.RqlObject;
import com.dkhenry.RethinkDB.RqlQuery;
import com.dkhenry.RethinkDB.RqlQuery.GetField;
import com.dkhenry.RethinkDB.RqlQuery.Var;
import com.dkhenry.RethinkDB.errors.RqlDriverException;
import com.foobarsite.rss.common.DateTools;
import com.foobarsite.rss.common.StringTools;
import com.foobarsite.rss.common.UrlTools;
import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.IFeedItem;
import com.foobarsite.rss.feed.Pair;
import com.foobarsite.rss.feed.desc.FeedItemProperty;
import com.foobarsite.rss.feed.desc.FeedProperty;
import com.foobarsite.rss.feed.desc.ITable;
import com.foobarsite.rss.feed.impl.FeedImpl;
import com.foobarsite.rss.feed.impl.FeedItemImpl;

public abstract class RethinkDBDAO extends CommonReader implements IFeedReader {
	public static final Logger LOG = LoggerFactory.getLogger(RethinkDBDAO.class);
	public static final String LEFT_JOIN = "left";
	public static final String RIGHT_JOIN = "right";

	public static final String ATTR_GENERATED_KEYS = "generated_keys";
	public static final String ATTR_REPLACED = "replaced";
	public static final String ATTR_UNCHANGED = "unchanged";
	public static final String ATTR_SKIPPED = "skipped";
	public static final String ATTR_ERRORS = "errors";

	private final HashMap<String,Object> feedIdIndex = new HashMap<String,Object>();
	private final HashMap<String,Object> urlIndex = new HashMap<String,Object>();

	private static ThreadLocal<RqlConnection> connectionTL = null;

	public RethinkDBDAO() {
		connectionTL = new ThreadLocal<RqlConnection>() {

			@Override
			protected RqlConnection initialValue() {
				RqlConnection connection = null;
				LOG.info("Creating new connection to {}:{}", getServerHost(), getServerPort());
				try {
					connection = RqlConnection.connect(getServerHost(), getServerPort());
				} catch (RqlDriverException e) {
					LOG.error(e.getMessage(), e);
				}
				return connection;
			}

			@Override
			public void remove() {
				LOG.info("Closing connection to {}:{}", getServerHost(), getServerPort());
				RqlConnection connection = get();
				if (connection != null) {
					try {
						connection.close();
					} catch (RqlDriverException e) {
						LOG.error(e.getMessage(), e);
					}
				}
				super.remove();
			};
		};

		feedIdIndex.put("index", "feed_id");
		urlIndex.put("index", "url");
	}

	@Override
	public int updateFeeds(List<String> ids) {
		int counter = 0;
		long time = System.currentTimeMillis();
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get_all(ids.toArray()));
			for(RqlObject ro : cursor) {
				List<Object> list = ro.getList();
				for (Object object : list) {
					Map<String,Object> m = (Map<String,Object>)object;
					IFeed feed = buildFeed(m);
					counter++;
					if (feed != null) {
						loadFeedItems(feed);
					}
				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("{} feeds updated in {} ms.", counter, (System.currentTimeMillis() - time));
		return counter;
	}

	@Override
	public IFeed loadFeedItems(IFeed feed) {
		LOG.info("Reading url '{}'", (feed == null ? "NULL" : feed.getUrl()));
		if (feed == null) {
			return feed;
		}

		Pair<Boolean, Boolean> result = canUpdate(feed.getGenId());
		if (result.getValue1() || !result.getValue2()) { // can update || feed is new
			try {
				long time = System.currentTimeMillis();
				feed = fetch(feed);
				feed = save(feed);
				saveItems(feed);
				LOG.info("url '{}' saved in {} ms.", feed.getUrl(), (System.currentTimeMillis() - time));
			} catch (Exception ex) {
				feed.setLastFailedFetch(new Date());
				feed.setLastFailMsg(getStackTraceMessage(ex));
				feed = save(feed);
				LOG.error("Failed at reading feed. Message: {}", ex.getMessage(), ex);
			}
		}
		else if (!feed.getFeedItems().isEmpty()) {
			LOG.info("Feed '{}' exists in db, feed contains {} items for this time, skipping..", feed.getName(), feed.getFeedItems().size());
		}
		return feed;
	}

	@Override
	public int getRowCountFor(ITable table) {
		Double count = null;
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(table.getTableName()).count());
			for(RqlObject o: cursor) {
				count = o.getNumber();
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		} catch (NumberFormatException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("Count of objects to table {} is {}", table, count);
		return count == null ? 0 : count.intValue();
	}

	@Override
	public List<String> getProperties(ITable property) {
		long time = System.currentTimeMillis();
		List<String> ids = new ArrayList<>();
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(property.getTableName()).pluck(property.toString()));
			for(RqlObject o : cursor) {
				Map<String,Object> m = o.getMap();
				Object prop = m.get(property.toString());
				if (prop != null) {
					ids.add(prop.toString());
				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		}
		LOG.info("Loaded {} {} {}s in {} ms.", ids.size(), property.getTableName(), property.toString(), (System.currentTimeMillis() - time));
		return ids;
	}

	@Override
	public Pair<Boolean, Boolean> canUpdate(final String feedId) {
		boolean canUpdate = false;
		boolean found = false;
		Date lastOkFetch = DEFAULT_DATE_MIN;
		Date lastFailedFetch = DEFAULT_DATE_MAX;
		String lastFailMsg = null;
		String lastFailResp = null;

		if (!StringTools.isBlankOrNull(feedId)) {
			try {
				RqlConnection r = getConnection();
				RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get(feedId));

				for(RqlObject o: cursor) {
					Map<String,Object> m = o.getMap();
					lastOkFetch = getDate(m, FeedProperty.LAST_OK_FETCH, DEFAULT_DATE_MIN);
					lastFailedFetch = getDate(m, FeedProperty.LAST_FAILED_FETCH, DEFAULT_DATE_MAX);
					lastFailMsg = getStr(m, FeedProperty.LAST_FAIL_MSG, "");
					lastFailResp = getStr(m, FeedProperty.LAST_FAIL_RESPONSE, null);
					found = true;
				}

				if (found) {
					Calendar cal = Calendar.getInstance();
					cal.setTime(lastOkFetch);
					cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) + fetchInterval[0]);
					cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + fetchInterval[1]);
					cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + fetchInterval[2]);
					canUpdate = cal.getTime().before(new Date());

					if (canUpdate && !DateTools.equalsIgnoreMinsSecs(lastFailedFetch, DEFAULT_DATE_MAX)) {
						cal.setTime(lastFailedFetch);
						cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH) + failfetchInterval[0]);
						cal.set(Calendar.HOUR_OF_DAY, cal.get(Calendar.HOUR_OF_DAY) + failfetchInterval[1]);
						cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + failfetchInterval[2]);
						canUpdate = cal.getTime().before(new Date());
					}
				}

			} catch (RqlDriverException e) {
				LOG.error(e.getMessage(), e);
				handleDriverException(e);
			} catch (NumberFormatException e) {
				LOG.error(e.getMessage(), e);
			}
		}

		if (!found) {
			LOG.warn("No feeds found with id {}", feedId);
		}
		else {
			LOG.info((canUpdate ? "Can" : "Can't") + " update items for feed {}, fetched: {} (failed {} {} {}) ",
					feedId, DateTools.formatLongDate(lastOkFetch), DateTools.formatShortDate(lastFailedFetch),
					StringTools.nullToEmpty(lastFailResp),
					StringTools.cutToLength(lastFailMsg, 80));
		}
		return new Pair<Boolean, Boolean>(canUpdate, found);
	}

	/**
	 * r.table('blog_item').getAll('cf0c31b6-efe6-4ae3-b383-a94cc5d3feae', {index: "feed_id"}).filter( function(item) {
	 *     return item("link").match("raibledesigns.com/rd/entry/the_modern_java_web_developer$")
	 * })
	 */
	@Override
	public IFeedItem getExistingFeedItem(final String feedid, String link, String uri, String title) {
		List<IFeedItem> items = new ArrayList<>();
		try {
			RqlConnection r = getConnection();
			RqlQuery query = r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).get_all(feedid).optargs(feedIdIndex);
			Object filterParam = null;
			if (!StringTools.isBlankOrNull(uri)) {
				if (UrlTools.isValidUrl(uri)) {
					filterParam = getLinkMatch(FeedItemProperty.URI, uri.trim());
				}
				else {
					filterParam = getMap(FeedItemProperty.URI, uri.trim());
				}
			}
			else if (!StringTools.isBlankOrNull(link)) {
				filterParam = getLinkMatch(FeedItemProperty.LINK, link.trim());
			}
			else if (!StringTools.isBlankOrNull(title)) {
				filterParam = getMap(FeedItemProperty.TITLE, title.trim());
			}
			else {
				LOG.error("No search terms for existing feed for '{}' !", feedid);
				return null;
			}

			RqlCursor cursor = r.run(query.filter(filterParam));
			for(RqlObject o: cursor) {
				Map<String,Object> m = o.getMap();
				items.add(buildFeedItem(m, false));
			}
			if (items.size() > 1) {
				LOG.error("There shouldn't be {} items for '{}' / '{}'", items.size(), feedid, link);
				for (IFeedItem item : items) {
					LOG.error("{} | {} | {}", item.getGenId(), item.getCreatedDate(), item.getLink());
				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		} catch (NoSuchElementException | NumberFormatException e) {
			LOG.error(e.getMessage(), e);
		}

		IFeedItem item = null;
		if (items.size() == 1) {
			return items.get(0);
		}
		else if (items.size() > 1) {
			for (IFeedItem i : items) {
				if (item == null || i.getCreatedDate().after(item.getCreatedDate())) {
					item = i;
				}
			}
		}
		return item;
	}

	@Override
	public IFeed loadFeed(String url) {
		IFeed feed = null;
		try {
			String u = UrlTools.trimUrl(url);
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).
					get_all(u, u.toLowerCase()).optargs(urlIndex));

			Set<IFeed> feeds = new HashSet<>();
			for(RqlObject o : cursor) {
				Map<String,Object> m = o.getMap();
				feeds.add(buildFeed(m));
			}
			if (feeds.size() > 1) {
				LOG.error("There shouldn't be more than one feed for '{}'", u);
			}
			if (!feeds.isEmpty()) {
				feed = feeds.iterator().next();
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		} catch (NoSuchElementException e) {
			LOG.error(e.getMessage(), e);
		}

		LOG.info("Loaded feed '{}', '{}' ", (feed == null ? "null" : feed.getGenId()), (feed == null ? "null" : feed.getName()));
		return feed;
	}

	/**
	 * r.table('blog_item').getAll("c3ec41c1-e3eb-48cd-ab8c-b33965d001e2", {index: 'feed_id'}).eqJoin("feed_id", r.table("blog"))
	 * r.table('blog').getAll("c3ec41c1-e3eb-48cd-ab8c-b33965d001e2").eqJoin("id", r.table("blog_item"), {index: 'feed_id'})
	 * 
	 * RqlCursor cursor = r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).
	 * 	get_all(ids.toArray()).optargs(feedIdIndex).eq_join(FeedItemProperty.FEED_ID.toString(), r.table(FeedProperty.TABLE_NAME.toString())));
	 */
	@Override
	public List<IFeed> loadFeeds(List<String> ids, boolean loadContent) {
		long time = System.currentTimeMillis();
		Map<String, IFeed> feedMap = new HashMap<>();
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).
					get_all(ids.toArray()).eq_join(FeedProperty.GEN_ID.toString(), r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString())).optargs(feedIdIndex));

			for(RqlObject o : cursor) {
				List<Object> list = o.getList();
				for (Object object : list) {
					Map<String,Object> m = (Map<String,Object>)object;
					Map<String,Object> left = (Map<java.lang.String, Object>) m.get(LEFT_JOIN);
					Map<String,Object> right = (Map<java.lang.String, Object>) m.get(RIGHT_JOIN);

					IFeed feed = null;
					String key = left.get(FeedProperty.GEN_ID.toString()).toString();
					if (!feedMap.containsKey(key)) {
						feed = buildFeed(left);
						feedMap.put(feed.getGenId(), feed);
					}
					else {
						feed = feedMap.get(key);
					}
					feed.getFeedItems().add(buildFeedItem(right, loadContent));
				}
			}

			List<String> notJoined = new ArrayList<>();
			for (String id : ids) {
				if (!feedMap.containsKey(id)) {
					notJoined.add(id);
				}
			}
			if (!notJoined.isEmpty()) {
				cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get_all(notJoined.toArray()));
				for(RqlObject o: cursor) {
					List<Object> list = o.getList();
					for (Object object : list) {
						Map<String,Object> m = (Map<String,Object>)object;
						IFeed feed = buildFeed(m);
						feedMap.put(feed.getGenId(), feed);
					}
				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		}

		List<IFeed> feeds = new ArrayList<>();
		for (String key : feedMap.keySet()) {
			feeds.add(feedMap.get(key));
		}
		LOG.info("Loaded {} feeds, with ids '{}' in {} ms.", feeds.size(), ids.toString(), (System.currentTimeMillis() - time));
		return feeds;
	}

	@Override
	public IFeed save(IFeed feed) {
		long time = System.currentTimeMillis();
		try {
			RqlConnection r = getConnection();
			Map<String, Object> values = new HashMap<>();
			putStr(values, FeedProperty.NAME, feed.getName());
			putStr(values, FeedProperty.URL, UrlTools.trimUrl(feed.getUrl()));
			putNonBlankStr(values, FeedProperty.URI, feed.getUri());
			putNonBlankStr(values, FeedProperty.DESCRIPTION, feed.getDescription());
			putNonBlankStr(values, FeedProperty.AUTHOR, feed.getAuthor());
			putNonBlankStr(values, FeedProperty.COPYRIGHT, feed.getCopyright());
			putNonBlankStr(values, FeedProperty.LANGUAGE, feed.getLanguage());
			putNonBlankStr(values, FeedProperty.IMAGE_URL, feed.getImageUrl());
			putNonBlankStr(values, FeedProperty.CATEGORIES, feed.getCategories());
			putDate(values, FeedProperty.LAST_FAILED_FETCH, feed.getLastFailedFetch(), DEFAULT_DATE_MAX);
			putNonBlankStr(values, FeedProperty.LAST_FAIL_MSG, feed.getLastFailMsg());
			putNonBlankStr(values, FeedProperty.LAST_FAIL_RESPONSE, feed.getLastFailResponseCode());
			putDate(values, FeedProperty.LAST_OK_FETCH, feed.getLastOkFetch(), DEFAULT_DATE_MIN);
			if (StringTools.isBlankOrNull(feed.getGenId())) {
				long currentTime = System.currentTimeMillis();
				put(values, FeedProperty.CREATED, currentTime);
				put(values, FeedProperty.UPDATED, currentTime);
				RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).insert(values));
				List<String> id = cursor.next().getAs(ATTR_GENERATED_KEYS);
				feed.setGenId(id.get(0));
				LOG.info("Inserted feed {} in {} ms", id, (System.currentTimeMillis() - time));
			}
			else {
				put(values, FeedProperty.UPDATED, System.currentTimeMillis());
				r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get(feed.getGenId()).update(values));
				LOG.info("Updated feed {} in {} ms", feed.getGenId(), (System.currentTimeMillis() - time));
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		}
		return feed;
	}

	@Override
	public List<IFeedItem> saveItems(IFeed feed) {
		long time = System.currentTimeMillis();
		List<IFeedItem> items = new ArrayList<>();
		try {
			RqlConnection r = getConnection();
			List<String> inserted = new ArrayList<>(feed.getFeedItems().size());
			List<String> updated = new ArrayList<>(feed.getFeedItems().size());
			for (IFeedItem item : feed.getNewFeedItems()) {
				Map<String, Object> values = createMap(item, feed.getGenId());
				final IFeedItem existingItem = getExistingFeedItem(feed.getGenId(), item.getLink(), item.getUri(), item.getTitle());
				if (existingItem == null) {
					RqlCursor cursor = r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).insert(values));
					List<String> id = cursor.next().getAs(ATTR_GENERATED_KEYS);
					item.setGenId(id.get(0));
					inserted.add(item.getGenId());
					items.add(item);
				}
				else if (DateTools.isFirstAfterSecond(item.getUpdatedDate(), existingItem.getUpdatedDate())) {
					r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).get(existingItem.getGenId()).update(values));
					item.setGenId(existingItem.getGenId());
					updated.add(existingItem.getGenId());
				}
				else {
					item.setGenId(existingItem.getGenId());
				}
				feed.getFeedItems().add(item);
			}
			LOG.info("Inserted {} Updated {} of {}. New: '{}' Updated: '{}' in {} ms", inserted.size(), updated.size(),
					feed.getFeedItems().size(), inserted.toString(), updated.toString(), (System.currentTimeMillis() - time));
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		} finally {
			feed.getNewFeedItems().clear();
		}
		return items;
	}

	@Override
	public boolean saveItem(IFeedItem item) {
		boolean success = false;
		long time = System.currentTimeMillis();
		try {
			if (!StringTools.isBlankOrNull(item.getGenId()) && !StringTools.isBlankOrNull(item.getFeedId())) {
				RqlConnection r = getConnection();
				Map<String, Object> values = createMap(item, item.getFeedId());
				r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).get(item.getGenId()).update(values));
				success = true;
				LOG.info("Updated item {}/{} in {} ms.", item.getGenId(), item.getFeedId(), (System.currentTimeMillis() - time));
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		}
		return success;
	}

	@Override
	public boolean deleteItem(IFeedItem feed) {
		boolean success = false;
		try {
			RqlConnection r = getConnection();
			if (StringTools.isBlankOrNull(feed.getGenId())) {
				Map<String, Object> values = new HashMap<>();
				put(values, FeedItemProperty.FEED_ID, feed.getFeedId());
				putStr(values, FeedItemProperty.LINK, feed.getLink());
				r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).filter(values).delete());
			}
			else {
				r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).get(feed.getGenId()).delete());
			}
			LOG.info("Deleted item {}/{}", feed.getFeedId(), feed.getGenId());
			success = true;
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
			handleDriverException(e);
		}
		return success;
	}

	@Override
	public int trimFeedUrlSlashes(List<String> ids) {
		int counter = 0;
		int updateCounter = 0;
		long timeAll = System.currentTimeMillis();
		long timeBatch = System.currentTimeMillis();
		int batchSize = 100;
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get_all(ids.toArray()));
			for(RqlObject ro : cursor) {
				List<Object> list = ro.getList();
				for (Object object : list) {
					Map<String,Object> m = (Map<String,Object>)object;
					IFeed feed = buildFeed(m);
					if (!StringTools.isBlankOrNull(feed.getUrl()) && feed.getUrl().trim().endsWith("/")) {
						RqlConnection r2 = getConnection();
						Map<String, Object> values = new HashMap<>();
						putStr(values, FeedProperty.URL, UrlTools.trimUrl(feed.getUrl()));
						r2.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()).get(feed.getGenId()).update(values));
						updateCounter++;
					}
					counter++;
					if (counter % batchSize == 0) {
						LOG.info("{} of {} feeds handled in {} ms.", batchSize, counter, (System.currentTimeMillis() - timeBatch));
						timeBatch = System.currentTimeMillis();
					}
				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("{} items of {} trimmed in {} ms.", updateCounter, ids.size(), (System.currentTimeMillis() - timeAll));
		return counter;
	}

	@Override
	public int trimItemWhiteSpace(List<String> ids) {
		int counter = 0;
		long timeAll = System.currentTimeMillis();
		long timeBatch = System.currentTimeMillis();
		int batchSize = 1000;
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedItemProperty.TABLE_NAME.toString()).get_all(ids.toArray()));
			for(RqlObject ro : cursor) {
				List<Object> list = ro.getList();
				for (Object object : list) {
					Map<String,Object> m = (Map<String,Object>)object;
					IFeedItem item = buildFeedItem(m, true);
					saveItem(item);
					counter++;
					if (counter % batchSize == 0) {
						LOG.info("{} of {} items trimmed in {} ms.", batchSize, counter, (System.currentTimeMillis() - timeBatch));
						timeBatch = System.currentTimeMillis();
					}

				}
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("{} items trimmed in {} ms.", counter, (System.currentTimeMillis() - timeAll));
		return counter;
	}

	/** Test for duplicate feed items by url and uri */
	@Override
	public void testFeedItems() {
		long time = System.currentTimeMillis();
		int itemCounter = 0;
		int feedCounter = 0;
		boolean allOk = true;
		List<String> messages = new ArrayList<>();
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()));

			for(RqlObject o : cursor) {
				String genId = get(o.getMap(), FeedProperty.GEN_ID).toString();
				List<IFeed> feeds = loadFeeds(Arrays.asList(genId), true);
				if (feeds.size() != 1) {
					LOG.error("Error loading feed with {}", genId);
					return;
				}
				Map<String, IFeedItem> linkToItem = new HashMap<>();
				Map<String, IFeedItem> uriToItem = new HashMap<>();
				Map<String, IFeedItem> titlePubContToItem = new HashMap<>();
				IFeed f = feeds.get(0);
				for (IFeedItem item : f.getFeedItems()) {

					if (UrlTools.isValidUrl(item.getLink())) {
						String link = UrlTools.getHost(item.getLink());
						if (linkToItem.containsKey(link)) {
							IFeedItem existingItem = linkToItem.get(link);
							allOk = false;

							String l1 = UrlTools.isValidUrl(existingItem.getLink()) ? UrlTools.getHost(existingItem.getLink()) : existingItem.getLink();
							String l2 = UrlTools.isValidUrl(item.getLink()) ? UrlTools.getHost(item.getLink()) : item.getLink();
							String u1 = UrlTools.isValidUrl(existingItem.getUri()) ? UrlTools.getHost(existingItem.getUri()) : existingItem.getUri();
							String u2 = UrlTools.isValidUrl(item.getUri()) ? UrlTools.getHost(item.getUri()) : item.getUri();
							String c1 = existingItem.getContent();
							String c2 = item.getContent();
							if (areEquals(existingItem.getTitle(), item.getTitle()) && areEquals(l1, l2) && areEquals(u1, u2) && areEquals(c1, c2)) {
								IFeedItem itemToDelete = existingItem.getCreatedDate().after(item.getCreatedDate()) ? item : existingItem;
								messages.add(String.format("1. Items '%s','%s' equal, deleted created at %s for %s", item.getGenId(), existingItem.getGenId(), DateTools.formatLongDate(itemToDelete.getCreatedDate()), itemToDelete.getUri()));
								continue;
							}

							if (areEquals(existingItem.getTitle(), item.getTitle()) &&
									areEquals(l1, l2) && areEquals(u1, u2) &&
									DateTools.equalsIgnoreMinsSecs(existingItem.getPublishDate(), item.getPublishDate()) &&
									areEquals(existingItem.getAuthor(), item.getAuthor())) {
								IFeedItem itemToDelete = existingItem.getCreatedDate().after(item.getCreatedDate()) ? item : existingItem;
								messages.add(String.format("2. Items '%s','%s' equal, deleted created at %s for %s", item.getGenId(), existingItem.getGenId(), DateTools.formatLongDate(itemToDelete.getCreatedDate()), itemToDelete.getUri()));
								continue;
							}

							messages.add(String.format("Duplicate items '%s','%s', for URL '%s'", item.getGenId(), existingItem.getGenId(), link));
						}
						else {
							linkToItem.put(link, item);
						}
					}

					String titlePubContKey = item.getTitle() + (item.getPublishDate() == null ? DateTools.DEFAULT_DATE_MAX.getTime() : item.getPublishDate().getTime()) + item.getContent().hashCode();
					if (titlePubContToItem.containsKey(titlePubContKey) && StringTools.areEqualsAlsoNulls(titlePubContToItem.get(titlePubContKey).getUri(), item.getUri())) {
						IFeedItem existingItem = titlePubContToItem.get(titlePubContKey);
						messages.add(String.format("Duplicate items '%s','%s', for title+pub+cont '%s'", item.getGenId(), existingItem.getGenId(), item.getLink()));
						allOk = false;

						String host = StringTools.isBlankOrNull(existingItem.getLink()) ? "" : UrlTools.getBaseUrl(existingItem.getLink());

						if (host.equals("http://www.jpl.nasa.gov") || host.equals("http://blog.tmorris.net")) {
							IFeedItem itemToDelete = StringTools.isBlankOrNull(existingItem.getUri()) ? existingItem :
								StringTools.isBlankOrNull(item.getUri()) ? item : null;
							if (itemToDelete != null) {
								LOG.info("Deleting duplicate item '{}' without uri, link {}", itemToDelete.getGenId(), itemToDelete.getLink());
								deleteItem(itemToDelete);
								titlePubContToItem.remove(titlePubContKey);
							}
						}
					}
					else {
						titlePubContToItem.put(titlePubContKey, item);
					}

					String uri = item.getUri();
					if (!StringTools.isBlankOrNull(uri)) {
						if (uriToItem.containsKey(uri)) {
							IFeedItem existingItem = uriToItem.get(uri);
							messages.add(String.format("Duplicate items '%s','%s', for URI '%s'", item.getGenId(), existingItem.getGenId(), uri));
							allOk = false;
						}
						else {
							uriToItem.put(uri, item);
						}
					}
					itemCounter++;
				}
				feedCounter++;
			}
		} catch (RqlDriverException | IOException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("Loaded {} feed items of {} feeds in {} ms. {}", itemCounter, feedCounter, (System.currentTimeMillis() - time), (allOk ? "No duplicates found." : "Duplicates found."));
	}

	/** Test for duplicate feed urls */
	@Override
	public void testFeeds() {
		LOG.info("Checking feed integrity..");
		long time = System.currentTimeMillis();
		int counter = 0;
		boolean allOk = true;
		try {
			RqlConnection r = getConnection();
			RqlCursor cursor = r.run(r.db(getDbName()).table(FeedProperty.TABLE_NAME.toString()));
			Map<String, IFeed> urlToFeed = new HashMap<>();
			for(RqlObject o : cursor) {
				Map<String,Object> m = o.getMap();
				IFeed feed = buildFeed(m);
				String key = feed.getUrl().toLowerCase();
				if (urlToFeed.containsKey(key)) {
					IFeed existingFeed = urlToFeed.get(key);
					LOG.info("Duplicate feed {} and {}, url {}", feed.getGenId(), existingFeed.getGenId(), key);
					allOk = false;
				}
				else {
					urlToFeed.put(key, feed);
				}
				counter++;
			}
		} catch (RqlDriverException e) {
			LOG.error(e.getMessage(), e);
		}
		LOG.info("Loaded {} feeds in {} ms. {}", counter, (System.currentTimeMillis() - time), (allOk ? "No duplicate urls found." : "Duplicate urls found."));
	}

	@Override
	public abstract String getDbName();

	public abstract int getServerPort();

	public abstract String getServerHost();

	protected RqlConnection getConnection() {
		return connectionTL.get();
	}

	protected void handleDriverException(RqlDriverException e) {
		LOG.warn("Driver Exception, trying to reconnect...");
		if (e.getMessage() == null || "null".equals(e.getMessage())) {
			connectionTL.remove();
		}
	}

	protected IFeed buildFeed(Map<String,Object> m) {
		IFeed feed = null;
		if (m != null) {
			feed = new FeedImpl(
					get(m, FeedProperty.GEN_ID).toString(),
					get(m, FeedProperty.NAME).toString(),
					get(m, FeedProperty.URL).toString());
			feed.setLastFailedFetch(getDate(m, FeedProperty.LAST_FAILED_FETCH, DEFAULT_DATE_MAX));
			feed.setLastOkFetch(getDate(m, FeedProperty.LAST_OK_FETCH, DEFAULT_DATE_MIN));
			feed.setAuthor(getStr(m, FeedProperty.AUTHOR, null));
			feed.setDescription(getStr(m, FeedProperty.DESCRIPTION, null));
			feed.setCopyright(getStr(m, FeedProperty.COPYRIGHT, null));
			feed.setLanguage(getStr(m, FeedProperty.LANGUAGE, null));
			feed.setImageUrl(getStr(m, FeedProperty.IMAGE_URL, null));
			feed.setCategories(getStr(m, FeedProperty.CATEGORIES, null));
			feed.setUpdated(getDate(m, FeedProperty.UPDATED, DEFAULT_DATE_MIN));
			feed.setUri(getStr(m, FeedProperty.URI, null));
		}
		return feed;
	}

	protected IFeedItem buildFeedItem(Map<String,Object> m, boolean loadContent) {
		IFeedItem item = null;
		if (m != null) {
			item = new FeedItemImpl(
					get(m, FeedItemProperty.GEN_ID).toString(),
					get(m, FeedItemProperty.FEED_ID).toString(),
					get(m, FeedItemProperty.TITLE).toString(),
					(loadContent ? get(m, FeedItemProperty.CONTENT).toString() : ""),
					get(m, FeedItemProperty.LINK).toString(),
					get(m, FeedItemProperty.AUTHOR).toString(),
					getStr(m, FeedItemProperty.URI, null),
					getDate(m, FeedItemProperty.PUBLISHED, DEFAULT_DATE_MIN),
					getDate(m, FeedItemProperty.CREATED, DEFAULT_DATE_MIN),
					getDate(m, FeedItemProperty.UPDATED, null));
		}
		return item;
	}

	protected Map<String, Object> createMap(IFeedItem item, String feedId) {
		Map<String, Object> values = new HashMap<>();
		putStr(values, FeedItemProperty.LINK, item.getLink()); // links are case sensitive
		putNonBlankStr(values, FeedItemProperty.URI, item.getUri());
		putStr(values, FeedItemProperty.TITLE, item.getTitle());
		putStr(values, FeedItemProperty.CONTENT, item.getContent());
		putStr(values, FeedItemProperty.AUTHOR, item.getAuthor());
		putDate(values, FeedItemProperty.PUBLISHED, item.getPublishDate(), DEFAULT_DATE_MIN);
		put(values, FeedItemProperty.FEED_ID, feedId);
		if (StringTools.isBlankOrNull(item.getGenId())) {
			put(values, FeedItemProperty.CREATED, System.currentTimeMillis());
		}
		putDate(values, FeedItemProperty.UPDATED, item.getUpdatedDate(), null);
		return values;
	}

	@SuppressWarnings({ "hiding", "unchecked", "rawtypes" })
	protected <String,V> Map<String,V> getMap(Enum key, V val) {
		Map<String, V> map = new HashMap<String, V>();
		map.put((String) key.toString(), val);
		return map;
	}

	protected Object get(Map<String,Object> m, ITable property) {
		return m.get(property.toString());
	}

	protected String getStr(Map<String,Object> m, ITable property, String defaultVal) {
		Object prop = m.get(property.toString());
		return prop == null ? defaultVal : prop.toString();
	}

	protected Date getDate(Map<String,Object> m, ITable property, Date defaultVal) {
		Object prop = m.get(property.toString());
		return prop == null ? defaultVal : new Date(((Double)prop).longValue());
	}

	protected Object putStr(Map<String,Object> m, ITable property, String val) {
		return m.put(property.toString(), StringTools.trim(val));
	}

	protected Object put(Map<String,Object> m, ITable property, Object val) {
		return m.put(property.toString(), val);
	}

	protected Object putNonBlankStr(Map<String,Object> m, ITable property, String val) {
		return StringTools.isBlankOrNull(val) ? null : m.put(property.toString(), StringTools.trim(val));
	}

	protected Object putDate(Map<String,Object> m, ITable property, Date val, Date defaultDate) {
		if (val == null && defaultDate == null) {
			return null;
		}
		return m.put(property.toString(), val == null ? defaultDate.getTime() : val.getTime());
	}

	private Function getLinkMatch(final FeedItemProperty property, String link) {
		final String regExpLink = ("\\Q" + UrlTools.stripProtocol(link) + "\\E");
		return new Function() {
			@Override
			public RqlQuery apply(Var var) {
				return new GetField(var, property.toString()).match(regExpLink);
			}
		};
	}

}
