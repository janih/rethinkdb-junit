package com.foobarsite.rss.feed.impl;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.foobarsite.rss.common.StringTools;
import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.IFeedItem;

public class FeedImpl implements IFeed, Serializable {
	private static final long serialVersionUID = 1L;

	private String genId;
	private String name;
	private String uri;
	private String author;
	private String description;
	private final String url;
	private String copyright;
	private String language;
	private String imageUrl;
	private String categories;
	private final Set<IFeedItem> feedItems = new HashSet<>();
	private final Set<IFeedItem> newFeedItems = new HashSet<>();
	private Date updated;
	private Date lastOkFetch;
	private Date lastFailedFetch;
	private String lastFailMsg;
	private String lastFailResponseCode;

	public FeedImpl() {
		this(null, null);
	}

	public FeedImpl(String name, String url) {
		this(null, name, url);
	}

	public FeedImpl(String genId, String name, String url) {
		this.genId = genId;
		this.name = name;
		this.url = url;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getUrl() {
		return url;
	}

	@Override
	public Set<IFeedItem> getFeedItems() {
		return feedItems;
	}

	@Override
	public Set<IFeedItem> getNewFeedItems() {
		return newFeedItems;
	}

	@Override
	public String getGenId() {
		return genId;
	}

	@Override
	public void setGenId(String id) {
		this.genId = id;
	}

	@Override
	public Date getLastOkFetch() {
		return lastOkFetch;
	}

	@Override
	public void setLastOkFetch(Date lastOkFetch) {
		this.lastOkFetch = lastOkFetch;
	}

	@Override
	public Date getLastFailedFetch() {
		return lastFailedFetch;
	}

	@Override
	public void setLastFailedFetch(Date lastFailedFetch) {
		this.lastFailedFetch = lastFailedFetch;
	}

	@Override
	public String getAuthor() {
		return author;
	}

	@Override
	public void setAuthor(String author) {
		this.author = author;
	}

	@Override
	public String getUri() {
		return uri;
	}

	@Override
	public void setUri(String uri) {
		this.uri = uri;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String getCopyright() {
		return copyright;
	}

	@Override
	public void setCopyright(String copyright) {
		this.copyright = copyright;
	}

	@Override
	public String getLanguage() {
		return language;
	}

	@Override
	public void setLanguage(String language) {
		this.language = language;
	}

	@Override
	public String getImageUrl() {
		return imageUrl;
	}

	@Override
	public void setImageUrl(String imageUrl) {
		this.imageUrl = imageUrl;
	}

	@Override
	public String getCategories() {
		return categories;
	}

	@Override
	public void setCategories(String categories) {
		this.categories = categories;
	}

	@Override
	public void setLastFailMsg(String msg) {
		this.lastFailMsg = msg;
	}

	@Override
	public String getLastFailMsg() {
		return lastFailMsg;
	}

	@Override
	public String getLastFailResponseCode() {
		return lastFailResponseCode;
	}

	@Override
	public void setLastFailResponseCode(String lastFailResponseCode) {
		this.lastFailResponseCode = lastFailResponseCode;
	}

	@Override
	public Date getUpdated() {
		return updated;
	}

	@Override
	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	@Override
	public void clearItems() {
		getNewFeedItems().clear();
		getFeedItems().clear();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((genId == null) ? 0 : genId.hashCode());
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FeedImpl other = (FeedImpl) obj;
		if (genId == null) {
			if (other.genId != null)
				return false;
		} else if (!genId.equals(other.genId))
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	@Override
	public int compareTo(IFeed o) {
		return StringTools.nullToEmpty(this.name).compareTo(StringTools.nullToEmpty(o.getName()));
	}

	@Override
	public String toString() {
		return "FeedImpl [genId=" + genId + ", name=" + name + ", uri=" + uri + ", author="
				+ author + ", description=" + description + ", url=" + url + ", copyright="
				+ copyright + ", language=" + language + ", imageUrl=" + imageUrl + ", categories="
				+ categories + ", feedItems=" + feedItems.size() + ", updated=" + updated
				+ ", lastOkFetch=" + lastOkFetch + ", lastFailedFetch=" + lastFailedFetch
				+ ", lastFailMsg=" + lastFailMsg + "]";
	}

}
