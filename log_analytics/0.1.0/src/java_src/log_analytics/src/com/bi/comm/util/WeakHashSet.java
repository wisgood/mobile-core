package com.bi.comm.util;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.WeakHashMap;

public class WeakHashSet<E> extends AbstractSet<E> implements Set<E> {
	private transient WeakHashMap<E, Object> map;
	private static final Object PRESENT = new Object();

	public WeakHashSet() {
		this.map = new WeakHashMap<E, Object>();
	}

	public WeakHashSet(Collection<? extends E> c) {
		this.map = new WeakHashMap<E, Object>(Math.max(
				(int) (c.size() / .75f) + 1, 16));
		this.addAll(c);
	}

	public WeakHashSet(int initialCapacity, float loadFactor) {
		this.map = new WeakHashMap<E, Object>(initialCapacity, loadFactor);
	}

	public WeakHashSet(int initialCapacity) {
		this.map = new WeakHashMap<E, Object>(initialCapacity);
	}

	public Iterator<E> iterator() {
		return this.map.keySet().iterator();
	}

	public int size() {
		return this.map.size();
	}

	@Override
	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return this.map.containsKey(o);
	}

	@Override
	public boolean add(E o) {
		return this.map.put(o, WeakHashSet.PRESENT) == null;
	}

	@Override
	public boolean remove(Object o) {
		return this.map.remove(o) == WeakHashSet.PRESENT;
	}

	@Override
	public void clear() {
		this.map.clear();
	}

}
