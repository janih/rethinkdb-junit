package com.foobarsite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dkhenry.RethinkDB.RqlConnection;
import com.dkhenry.RethinkDB.errors.RqlDriverException;
import com.foobarsite.rss.common.StringTools;
import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.desc.FeedItemProperty;
import com.foobarsite.rss.feed.desc.FeedProperty;
import com.foobarsite.rss.feed.desc.UserFeedProperty;
import com.foobarsite.rss.server.IFeedReader;
import com.foobarsite.rss.server.RethinkDBDAO;
import com.google.common.collect.Lists;

/**
 * Test to reproduce https://github.com/rethinkdb/rethinkdb/issues/2410
 * 
 * @author janih
 */
public class Issue2410Test extends FeedServer {
	private IFeedReader reader = null;
	private String dbServerHost;
	private int dbServerPort;
	private int testHttpServerPort;
	private File feedDataFolder;
	private int partitionSize;
	private int threadCount;
	private int maxFeedCount;
	private int updateLoopCount;

	public Issue2410Test() {
		Properties p = new Properties();
		try {
			p.load(new FileReader(new File("junit.properties")));
			dbServerHost = p.getProperty("dbServerHost");
			dbServerPort = Integer.valueOf(p.getProperty("dbServerPort"));
			testHttpServerPort = Integer.valueOf(p.getProperty("testHttpServerPort"));
			feedDataFolder = new File(p.getProperty("feedDataFolder"));

			partitionSize = Integer.valueOf(p.getProperty("partitionSize"));
			threadCount = Integer.valueOf(p.getProperty("threadCount"));
			maxFeedCount = Integer.valueOf(p.getProperty("maxFeedCount"));
			updateLoopCount = Integer.valueOf(p.getProperty("updateLoopCount"));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	@Override
	protected int getServerPort() {
		return testHttpServerPort;
	}

	@Override
	@Before
	public void setup() {
		super.setup();

		reader = new RethinkDBDAO() {
			String db = new BigInteger(130, new SecureRandom()).toString(32);
			@Override
			public String getDbName() {
				return db;
			}
		};

		try {
			RqlConnection r = RqlConnection.connect(dbServerHost, dbServerPort);
			r.run(r.db_create(reader.getDbName()));
			r.run(r.db(reader.getDbName()).table_create(FeedItemProperty.TABLE_NAME));
			r.run(r.db(reader.getDbName()).table_create(FeedProperty.TABLE_NAME));
			r.run(r.db(reader.getDbName()).table_create(UserFeedProperty.TABLE_NAME));
			r.run(r.db(reader.getDbName()).table(FeedProperty.TABLE_NAME).index_create(FeedProperty.URL.toString())); // r.table('blog').indexCreate("url")
			r.run(r.db(reader.getDbName()).table(FeedItemProperty.TABLE_NAME).index_create(FeedItemProperty.FEED_ID.toString())); // r.table('blog_item').indexCreate("feed_id")
			r.run(r.db(reader.getDbName()).table(UserFeedProperty.TABLE_NAME).index_create(UserFeedProperty.FEED_ID.toString())); // r.table('userblog').indexCreate("feed_id")
			r.close();
		} catch (RqlDriverException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testCase1() {
		assertNotNull("HTTP server should not be null", server);
		assertTrue("HTTP server should running", server.isAlive());
		assertNotNull("Reader should not be null", reader);

		// 1. Insert
		List<IFeed> feedsToSave = getFeedList(feedDataFolder, new ArrayList<IFeed>());
		if (maxFeedCount > 0 && !feedsToSave.isEmpty()) {
			feedsToSave = Lists.partition(feedsToSave, maxFeedCount).get(0);
		}

		List<List<IFeed>> feedSublists = Lists.partition(feedsToSave, partitionSize);
		final ExecutorService executorService1 = Executors.newFixedThreadPool(threadCount);
		for (List<IFeed> sub : feedSublists) {
			executorService1.submit(new SaveFeeds(sub, reader));
		}
		executorService1.shutdown();
		try {
			executorService1.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}
		feedsToSave.clear();

		List<String> ids = reader.getProperties(FeedProperty.TABLE_NAME.getId());

		// 2. Trim slashes
		List<List<String>> idSublists = Lists.partition(ids, partitionSize);
		final ExecutorService executorService2 = Executors.newFixedThreadPool(threadCount);
		for (List<String> sub : idSublists) {
			executorService2.submit(new TrimFeeds(sub, reader));
		}
		executorService2.shutdown();
		try {
			executorService2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}

		for (int i = 0; i < updateLoopCount; i++) {
			// 3. Update
			final ExecutorService executorService3 = Executors.newFixedThreadPool(threadCount);
			for (List<String> sub : idSublists) {
				executorService3.submit(new FetchFeeds(sub, reader));
			}
			executorService3.shutdown();
			try {
				executorService3.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				fail(e.getMessage());
			}

			reader.setFetchInterval(0, 0, 0);

			// 4. Trim whitespace
			final ExecutorService executorService4 = Executors.newFixedThreadPool(threadCount);
			for (List<String> sub : idSublists) {
				executorService4.submit(new TrimWhitespace(sub, reader));
			}
			executorService4.shutdown();
			try {
				executorService4.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			} catch (InterruptedException e) {
				fail(e.getMessage());
			}

			// 5 Load documents..
			reader.testFeedItems();
			reader.testFeeds();
		}
	}

	@Override
	@After
	public void teardown() {
		super.teardown();
		try {
			RqlConnection r = RqlConnection.connect(dbServerHost, dbServerPort);
			r.run(r.db_drop(reader.getDbName()));
			r.close();
		} catch (RqlDriverException e) {
			fail(e.getMessage());
		}
	}

	private List<IFeed> getFeedList(File rootFolder, List<IFeed> feeds) {
		if (rootFolder.exists()) {
			for (File file : rootFolder.listFiles()) {
				if (file.isDirectory()) {
					getFeedList(file, feeds);
				}
				else {
					feeds.add(createFeed(file));
				}
			}
		}
		return feeds;
	}

	public static class SaveFeeds implements Runnable {
		private List<IFeed> feeds;
		private IFeedReader reader;

		public SaveFeeds(List<IFeed> feeds, IFeedReader reader) {
			this.feeds = feeds;
			this.reader = reader;
		}

		@Override
		public void run() {
			for (IFeed feed : feeds) {
				IFeed existing = reader.loadFeed(feed.getUrl());
				if (existing == null) {
					reader.save(feed);
				}
				else {
					feed = existing;
				}
				assertTrue("1. Feed should be succesfully saved", !StringTools.isBlankOrNull(feed.getGenId()));
			}
			assertEquals("1. Count should equal the list size", feeds.size(), reader.getRowCountFor(FeedProperty.TABLE_NAME));
		}
	}

	public static class TrimFeeds implements Runnable {
		private List<String> ids;
		private IFeedReader reader;

		public TrimFeeds(List<String> ids, IFeedReader reader) {
			this.ids = ids;
			this.reader = reader;
		}

		@Override
		public void run() {
			assertEquals(ids.size(), reader.trimFeedUrlSlashes(ids));
		}
	}

	public static class TrimWhitespace implements Runnable {
		private List<String> ids;
		private IFeedReader reader;

		public TrimWhitespace(List<String> ids, IFeedReader reader) {
			this.ids = ids;
			this.reader = reader;
		}

		@Override
		public void run() {
			assertEquals(ids.size(), reader.trimItemWhiteSpace(ids));
		}
	}

	public static class FetchFeeds implements Runnable {
		private List<String> ids;
		private IFeedReader reader;

		public FetchFeeds(List<String> ids, IFeedReader reader) {
			this.ids = ids;
			this.reader = reader;
		}

		@Override
		public void run() {
			assertEquals(ids.size(), reader.updateFeeds(ids));
		}
	}

}
