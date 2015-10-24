package com.github.heussd.lodicity.model;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataObjectIterable implements Iterable<DataObject> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataObjectIterable.class);

	private Class<? extends DataObject> dataObjectClass;
	int currentIndex = -1;
	List<HashMap<String, Object>> list;
	private Constructor<? extends DataObject> constructor;

	public DataObjectIterable(Class<? extends DataObject> dataObjectClass, List<HashMap<String, Object>> list) {
		this.dataObjectClass = dataObjectClass;
		this.list = list;

		try {
			this.constructor = dataObjectClass.getDeclaredConstructor(java.util.Map.class);
			this.constructor.setAccessible(true);
		} catch (Exception e) {
			throw new RuntimeException(dataObjectClass.getSimpleName() + " must specify a constructor for java.util.Map.class", e);
		}
		LOGGER.info("Constructed new Iterator for {} results", list.size());
	}

	@Override
	public Iterator<DataObject> iterator() {
		return new Iterator<DataObject>() {
			@Override
			public boolean hasNext() {
				return ++currentIndex < list.size();
			}

			@Override
			public DataObject next() {
				try {
					return constructor.newInstance(list.get(currentIndex));
				} catch (Exception e) {
					throw new RuntimeException(
							"Could not create a new instance of " + dataObjectClass.getSimpleName() + " from list item " + list.get(currentIndex), e);
				}
			}
		};
	}
}
