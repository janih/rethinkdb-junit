package com.foobarsite.rss.feed;

import java.io.Serializable;

public class Pair<T, V> implements Serializable {
	private static final long serialVersionUID = 1L;
	private final T v1;
	private final V v2;

	public Pair(T v1, V v2) {
		this.v1 = v1;
		this.v2 = v2;
	}

	public T getValue1() {
		return v1;
	}

	public V getValue2() {
		return v2;
	}

	@Override
	public String toString() {
		return "Pair [v1=" + v1 + ", v2=" + v2 + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((v1 == null) ? 0 : v1.hashCode());
		result = prime * result + ((v2 == null) ? 0 : v2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Pair<?, ?> other = (Pair<?, ?>) obj;
		if (v1 == null) {
			if (other.v1 != null) {
				return false;
			}
		} else if (!v1.equals(other.v1)) {
			return false;
		}
		if (v2 == null) {
			if (other.v2 != null) {
				return false;
			}
		} else if (!v2.equals(other.v2)) {
			return false;
		}
		return true;
	}

}
