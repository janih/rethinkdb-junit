package com.foobarsite.rss.server;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foobarsite.rss.common.DateTools;
import com.foobarsite.rss.common.StringTools;
import com.foobarsite.rss.common.UrlTools;
import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.IFeedItem;
import com.foobarsite.rss.feed.impl.FeedItemImpl;
import com.rometools.fetcher.FetcherException;
import com.rometools.fetcher.impl.FeedFetcherCache;
import com.rometools.fetcher.impl.HttpClientFeedFetcher;
import com.rometools.fetcher.impl.SyndFeedInfo;
import com.rometools.modules.content.ContentModule;
import com.rometools.rome.feed.module.DCModule;
import com.rometools.rome.feed.module.Module;
import com.rometools.rome.feed.synd.SyndCategory;
import com.rometools.rome.feed.synd.SyndCategoryImpl;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndFeedImpl;
import com.rometools.rome.feed.synd.SyndImage;
import com.rometools.rome.feed.synd.SyndImageImpl;
import com.rometools.rome.io.FeedException;

public abstract class CommonReader implements FeedFetcherCache {
	public static final Logger LOG = LoggerFactory.getLogger(CommonReader.class);
	public static final int READ_TIMEOUT = 60000;
	public static final int CONNECT_TIMEOUT = 20000;
	public static final List<String> CONTENT_TYPES = Arrays.asList("text/html", "html", "text/plain", "text", "text/xhtml", "xhtml");

	protected int[] fetchInterval = { 0, 4, 0};
	protected int[] failfetchInterval = { 7, 0, 0};
	private final HttpClientFeedFetcher feedFetcher;

	public CommonReader() {
		feedFetcher = new HttpClientFeedFetcher(this);
		feedFetcher.setConnectTimeout(CONNECT_TIMEOUT);
		feedFetcher.setReadTimeout(READ_TIMEOUT);
	}

	/**
	 * Feed check interval
	 * @param days
	 * @param hours
	 * @param minutes
	 */
	public void setFetchInterval(int days, int hours, int minutes) {
		this.fetchInterval = new int[] { days, hours, minutes };
	}

	/**
	 * Failed feed check interval
	 * @param days
	 * @param hours
	 * @param minutes
	 */
	public void setFailFetchInterval(int days, int hours, int minutes) {
		this.failfetchInterval = new int[] { days, hours, minutes };
	}

	public IFeed fetch(IFeed feed) throws MalformedURLException {
		URL feedUrl = new URL(UrlTools.trimUrl(feed.getUrl()));
		SyndFeed syndFeed;
		try {
			syndFeed = feedFetcher.retrieveFeed(feedUrl);
		} catch (FetcherException | FeedException | IOException |  IllegalArgumentException e) {
			LOG.warn("Failed at reading '{}': {}", feedUrl, getStackTraceMessage(e));
			feed.setLastFailedFetch(new Date());
			feed.setLastFailMsg(getStackTraceMessage(e));
			return feed;
		}

		List<SyndEntry> entries = syndFeed.getEntries();
		for (SyndEntry syndEntry : entries) {
			String title = StringTools.nullToEmpty(syndEntry.getTitle());
			String content = StringTools.nullToEmpty(parseContent(syndEntry));
			String link = StringTools.nullToEmpty(syndEntry.getLink());
			String author = StringTools.nullToEmpty(syndEntry.getAuthor());
			String uri = StringTools.nullToEmpty(syndEntry.getUri());
			Date published = syndEntry.getPublishedDate() == null ? DateTools.DEFAULT_DATE_MIN : syndEntry.getPublishedDate();
			Date updated = syndEntry.getUpdatedDate();
			feed.getNewFeedItems().add(new FeedItemImpl("", feed.getGenId(), title, content, link, author, uri, published, new Date(), updated));
		}

		if (!StringTools.isBlankOrNull(syndFeed.getTitle())) {
			feed.setName(syndFeed.getTitle());
		}
		feed.setDescription(syndFeed.getDescription());
		feed.setAuthor(syndFeed.getAuthor());
		feed.setCopyright(syndFeed.getCopyright());
		if (syndFeed.getImage() != null) {
			feed.setImageUrl(syndFeed.getImage().getUrl());
		}
		feed.setLanguage(syndFeed.getLanguage());
		feed.setLastOkFetch(new Date());
		if (!syndFeed.getCategories().isEmpty()) {
			StringBuilder builder = new StringBuilder();
			for (SyndCategory cat : syndFeed.getCategories()) {
				builder.append(cat.getName()).append(";");
			}
			feed.setCategories(builder.toString());
		}

		return feed;
	}

	public abstract IFeed loadFeed(String url);

	@Override
	public SyndFeedInfo getFeedInfo(URL feedUrl) {
		IFeed feed = loadFeed(feedUrl.toString());
		if (feed != null) {
			SyndFeedInfo feedInfo = new SyndFeedInfo();
			feedInfo.setLastModified(feed.getUpdated().getTime());
			feedInfo.setUrl(feedUrl);
			feedInfo.setId(feed.getUrl());
			SyndFeed syndFeed = new SyndFeedImpl();
			syndFeed.setAuthor(feed.getAuthor());
			syndFeed.setDescription(feed.getDescription());
			syndFeed.setCopyright(feed.getCopyright());
			if (!StringTools.isBlankOrNull(feed.getImageUrl())) {
				SyndImage image = new SyndImageImpl();
				image.setUrl(feed.getImageUrl());
				syndFeed.setImage(image);
			}
			syndFeed.setLanguage(feed.getLanguage());
			if (!StringTools.isBlankOrNull(feed.getCategories())) {
				String[] categories = feed.getCategories().split(";");
				List<SyndCategory> categoryList = new ArrayList<>();
				for (String category : categories) {
					SyndCategory cat = new SyndCategoryImpl();
					cat.setName(category);
					categoryList.add(cat);
				}
				syndFeed.setCategories(categoryList);
			}

			List<SyndEntry> entries = new ArrayList<>();
			for (IFeedItem item : feed.getFeedItems()) {
				SyndEntry entry = new SyndEntryImpl();
				entry.setTitle(item.getTitle());
				List<SyndContent> contents = new ArrayList<>();
				SyndContent content = new SyndContentImpl();
				content.setValue(item.getContent());
				content.setType(CONTENT_TYPES.get(0));
				contents.add(content);
				entry.setContents(contents);
				entry.setLink(item.getLink());
				entry.setAuthor(item.getAuthor());
				entry.setUri(item.getUri());
				entry.setPublishedDate(item.getPublishDate());
				entry.setUpdatedDate(item.getUpdatedDate());
				entries.add(entry);
			}
			syndFeed.setEntries(entries);

			feedInfo.setSyndFeed(syndFeed);
			return feedInfo;
		}
		else {
			return null;
		}
	}

	@Override
	public void setFeedInfo(URL feedUrl, SyndFeedInfo syndFeedInfo) {
		// Do nothing, feed gets saved later
	}

	@Override
	public void clear() {
		// Do nothing
	}

	@Override
	public SyndFeedInfo remove(URL feedUrl) {
		// Do nothing
		return null;
	}

	protected String nullToEmpty(Object obj) {
		return obj == null ? "" : obj.toString();
	}

	protected void addToSetStr(Set<String> set, String idString) {
		if (!StringTools.isBlankOrNull(idString)) {
			String[] ids = idString.split(";");
			for (String id : ids) {
				set.add(id);
			}
		}
	}

	private String parseContent(SyndEntry syndEntry) {
		String content = null;
		Module module = syndEntry.getModule(ContentModule.URI);
		if (module != null) {
			ContentModule cmod = (ContentModule) module;
			List encodeds = cmod.getEncodeds();
			if (encodeds != null && encodeds.size() > 0) {
				content = (String) encodeds.get(0);
			}
		}

		if (content == null) {
			int type = Integer.MAX_VALUE;
			SyndContent syndContent = null;
			List<SyndContent> contents = syndEntry.getContents();
			if (contents != null) {
				for (SyndContent cont : contents) {
					int contType = cont.getType() == null ? -1 : CONTENT_TYPES.indexOf(cont.getType().toLowerCase());
					if (contType < type) {
						type = contType;
						syndContent = cont;
					}
				}
			}

			if (syndContent == null) {
				syndContent = syndEntry.getDescription();
			}

			if (syndContent != null) {
				content = syndContent.getValue();
			}
		}

		if (StringTools.isBlankOrNull(content)) {
			DCModule dcModule = (DCModule) syndEntry.getModule(DCModule.URI);
			if (dcModule != null) {
				content = dcModule.getDescription();
			}
			else {
				content = "";
			}
		}

		return content;
	}

	public static String getStackTraceMessage(Exception e) {
		if (e == null) {
			return "null";
		}
		return e.getClass().getName() + ": " + (e.getMessage() == null ? "" : e.getMessage());
	}
}
