package com.foobarsite.rss.feed.impl;

import java.io.Serializable;
import java.util.Date;

import com.foobarsite.rss.feed.IFeedItem;

public class FeedItemImpl implements Serializable, IFeedItem {
	private static final long serialVersionUID = 1L;

	private String genId;
	private final String feedId;
	private final String title;
	private final String content;
	private final String link;
	private final String author;
	private final String uri;
	private final Date publishDate;
	private final Date createdDate;
	private final Date updatedDate;
	private boolean read;
	private boolean liked;

	public FeedItemImpl() {
		this(null, null);
	}

	public FeedItemImpl(String genId, String feedIdKey) {
		this(genId, feedIdKey, null, null, null, null, null, null, null, null);
	}

	public FeedItemImpl(String genId, String feedId, String title, String content, String link, String author, String uri, Date publishDate, Date createdDate, Date updatedDate) {
		this.genId = genId;
		this.feedId = feedId;
		this.title = title;
		this.content = content;
		this.link = link;
		this.author = author;
		this.uri = uri;
		this.publishDate = publishDate;
		this.createdDate = createdDate;
		this.updatedDate = updatedDate;
	}

	@Override
	public String getFeedId() {
		return feedId;
	}

	@Override
	public String getLink() {
		return link;
	}

	@Override
	public String getTitle() {
		return title;
	}

	@Override
	public String getContent() {
		return content;
	}

	@Override
	public String getAuthor() {
		return author;
	}

	@Override
	public String getUri() {
		return uri;
	}

	@Override
	public Date getPublishDate() {
		return publishDate;
	}

	@Override
	public String getGenId() {
		return genId;
	}

	@Override
	public void setGenId(String genId) {
		this.genId = genId;
	}

	@Override
	public Date getCreatedDate() {
		return createdDate;
	}

	@Override
	public Date getUpdatedDate() {
		return updatedDate;
	}

	@Override
	public boolean isRead() {
		return read;
	}

	@Override
	public void setRead(boolean read) {
		this.read = read;
	}

	@Override
	public boolean isLiked() {
		return liked;
	}

	@Override
	public void setLiked(boolean liked) {
		this.liked = liked;
	}

	@Override
	public int compareTo(IFeedItem o) {
		return this.publishDate.compareTo(o.getPublishDate());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((genId == null) ? 0 : genId.hashCode());
		result = prime * result + ((link == null) ? 0 : link.hashCode());
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
		FeedItemImpl other = (FeedItemImpl) obj;
		if (genId == null) {
			if (other.genId != null)
				return false;
		} else if (!genId.equals(other.genId))
			return false;
		if (link == null) {
			if (other.link != null)
				return false;
		} else if (!link.equals(other.link))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "FeedItemImpl [genId=" + genId + ", feedId=" + feedId + ", title=" + title
				+ ", content=" + content + ", link=" + link + ", author=" + author + ", uri=" + uri
				+ ", publishDate=" + publishDate + ", createdDate=" + createdDate
				+ ", updatedDate=" + updatedDate + ", read=" + read + ", liked=" + liked + "]";
	}

	public String toStringShort() {
		return "FeedItemImpl [genId=" + genId + ", link=" + link +
				", uri=" + uri + ", createdDate=" + createdDate
				+ ", updatedDate=" + updatedDate + "]";
	}

}
