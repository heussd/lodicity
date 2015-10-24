package com.github.heussd.lodicity.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.hibernate.EmptyInterceptor;
import org.hibernate.type.Type;
import org.json.JSONArray;

import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.model.Schema;

public class DataObjectInterceptor extends EmptyInterceptor {
	private static final long serialVersionUID = 1L;

	/**
	 * Make sure that {@link DataObject}-instances are correctly identified as {@link HashMap} instances. This is essential for using a dynamic model based on {@link DataObject}s.
	 * The original idea is presented in <a href="https://forum.hibernate.org/viewtopic.php?f=1&t=992446">https://forum.hibernate.org/viewtopic.php?f=1&t=992446</a>.
	 * 
	 * @param object
	 * @return
	 */
	@Override
	public String getEntityName(Object object) {
		if (object != null && object instanceof DataObject) {
			return object.getClass().getName();
		} else {
			return super.getEntityName(object);
		}
	}

	@Override
	public boolean onSave(Object entity, Serializable id, Object[] state, String[] propertyNames, Type[] types) {
		state = embedListsAsJson(entity, state, propertyNames, types);
		state = embedDataObjectsAsJson(entity, state, propertyNames, types);
		return super.onSave(entity, id, state, propertyNames, types);
	}

	@Override
	public boolean onFlushDirty(Object entity, Serializable id, Object[] currentState, Object[] previousState, String[] propertyNames, Type[] types) {
		currentState = embedListsAsJson(entity, currentState, propertyNames, types);
		currentState = embedDataObjectsAsJson(entity, currentState, propertyNames, types);
		return super.onFlushDirty(entity, id, currentState, previousState, propertyNames, types);
	}

	private Object[] embedListsAsJson(Object entity, Object[] states, String[] propertyNames, Type[] types) {
		for (int i = 0; i < propertyNames.length; i++) {
			String propertyName = propertyNames[i];

			if (Schema.isListType((DataObject) entity, propertyName)) {
				Object value = states[i];
				value = (String) new JSONArray((List<String>) value).toString();
				states[i] = value;
			}
		}
		return states;
	}

	private Object[] embedDataObjectsAsJson(Object entity, Object[] states, String[] propertyNames, Type[] types) {
		for (int i = 0; i < propertyNames.length; i++) {
			Object value = states[i];

			if (value != null && value instanceof DataObject) {
				value = (String) ((DataObject) value).toJson();
				states[i] = value;
			}
		}
		return states;
	}
}
