package com.github.heussd.lodicity.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;

import org.json.JSONArray;
import org.json.simple.JSONValue;

@Entity
public class DataObject extends HashMap<String, Object> {

	private final static List<String> SCHEMA_IGNORED_ATTRIBUTES = Arrays.asList("_class_", "HibernateInternalId");

	private static final long serialVersionUID = 1L;

	private String hibernateInternalId;

	@Override
	public Object put(String attribute, Object value) {
		return put(attribute, value, true);
	}

	public Object put(String attribute, Object value, boolean validate) {
		Object previousValue = super.get(attribute);

		if (!SCHEMA_IGNORED_ATTRIBUTES.contains(attribute)) {
			if (validate)
				assert Schema.isValid(this, attribute, value);
		}

		super.put(attribute, value);
		return previousValue;
	}

	public <T> T get(String attribute) {
		return get(attribute, true);
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String attribute, int index) {
		assert Schema.isListType(this, attribute) : attribute + " is no list type, thus, the n-th list element cannot be retrieved.";
		return (T) this.<List<Object>> get(attribute).get(index);
	}

	@SuppressWarnings("unchecked")
	protected <T> T get(String attribute, boolean validate) {
		Object value = super.get(attribute);

		if (Schema.isListType(this.getClass(), attribute)) {
			// Value might already be converted
			if (value != null && value instanceof Collection) {
				return (T) value;
			}

			// Deserialize list from JSON object
			if (!(value instanceof ArrayList)) {
				String jsonString = (String) value;
				List<String> list = new ArrayList<String>();

				if (jsonString != null && !jsonString.equals("")) {
					// https://stackoverflow.com/questions/3395729/convert-json-array-to-normal-java-array
					JSONArray jsonArray = new JSONArray(jsonString);
					if (jsonArray != null) {
						int len = jsonArray.length();
						for (int i = 0; i < len; i++) {
							list.add(jsonArray.get(i).toString());
						}
					}
				}
				value = list;
			}
		}

		if (validate && ! SCHEMA_IGNORED_ATTRIBUTES.contains(attribute)) {
//			System.out.println(attribute + " " + value);
			assert Schema.isValid(this, attribute, value);
		}

		// Make sure lists always return != null
		if (value == null && Schema.isListType(this, attribute)) {
			value = new ArrayList<>();
		}

		return (T) value;
	}

	public static String generateId(String... identifierStrings) {
		return String.join("_", Arrays.asList(identifierStrings));
	}

	public String toJson() {
		return JSONValue.toJSONString(this);
	}

	public DataObject(Map<String, Object> map) {
		super(map);
	}

	// Needed for Hibernate

	public void setHibernateInternalId(String id) {
		this.hibernateInternalId = id;
	}

	public String getId() {
		return hibernateInternalId;
	}

	// Deprecated?

	public DataObject() {
		addClassAttribute();

		// /* Make sure lists are empty by default after DataObject initialization. */
		// for (String attribute : Schema.getAttributes(this)) {
		// if (!attribute.equals("_class_")) {
		// if (Schema.isListType(this, attribute)) {
		// this.setWithoutValidation(attribute, new ArrayList<>());
		// }
		// }
		// }
	}

	public void addClassAttribute() {
		this.put("_class_", this.getClass().getSimpleName());
	}

	public boolean isValid(String attribute, String value) {
		return Schema.isValid(this, attribute, value);
	}

	public void validate() {
		Schema.validate(this);
	}

	// Kept for compatiblity reasons
	public void set(String attribute, Object value) {
		put(attribute, value);
	}

	public <T> T getWithoutValidation(String attribute) {
		return this.<T> get(attribute);
	}

}
