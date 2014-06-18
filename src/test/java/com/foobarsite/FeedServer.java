package com.foobarsite;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.After;
import org.junit.Before;

import com.foobarsite.rss.feed.IFeed;
import com.foobarsite.rss.feed.impl.FeedImpl;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.Response.Status;

public class FeedServer {
	public static final int TEST_HTTP_SERVER_PORT = 11080;
	protected NanoHTTPD server = null;
	private Map<String, LinkedList<File>> files = new HashMap<>();

	protected int getServerPort() {
		return TEST_HTTP_SERVER_PORT;
	}

	@Before
	public void setup() {
		try {
			server = new NanoHTTPD(getServerPort()) {
				@Override
				public Response serve(IHTTPSession session) {
					FileInputStream input = null;
					try {
						synchronized (files) {
							LinkedList<File> list = files.get(session.getUri().replaceFirst("/", ""));
							if (list == null) {
								fail("List null for " + session.getUri() + " !");
							}
							File firstFile = list.pollFirst();
							if (firstFile == null) {
								fail("No file for in list for " + session.getUri() + " !");
							}
							input = new FileInputStream(firstFile);
							list.offerLast(firstFile);
						}
					} catch (FileNotFoundException e) {
						fail(e.getMessage());
					}
					return new Response(Status.OK, "text/html", input);
				}
			};
			server.start();
		} catch(Exception e) {
			fail(e.getMessage());
		}
	}

	protected IFeed createFeed(File file) {
		String name = file.getName();
		if (files.containsKey(name)) {
			files.get(name).addLast(file);
			Collections.sort(files.get(name));
		}
		else {
			LinkedList<File> list = new LinkedList<>();
			list.add(file);
			files.put(name, list);
		}
		return new FeedImpl(name, ("http://localhost:" + getServerPort() + "/" + name));
	}

	@After
	public void teardown() {
		if (server != null && server.isAlive()) {
			server.stop();
		}
	}
}
