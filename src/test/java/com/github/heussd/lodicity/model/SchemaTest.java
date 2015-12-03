package com.github.heussd.lodicity.model;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.junit.Test;

public class SchemaTest {

	private final static String COMPANION_STRING = "Hello this is a string with nasty characters äöü!";

	private DataObject makeCompanionDataObject() {
		DataObject dataObject = new DataObject();
		dataObject.set("string", COMPANION_STRING);
		dataObject.set("float", 1F);
		List<String> stringList = new ArrayList<>();

		stringList.add("Hello");
		stringList.add("World");
		dataObject.set("stringList", stringList);

		return dataObject;
	}

	@Test
	public void testListValidation() {
		DataObject dataObject = makeCompanionDataObject();
		dataObject.validate();
	}

	@Test
	public void testTypeSupport() {
		DataObject dataObject = new DataObject();
		assertEquals("String", Schema.getDataType(dataObject, "string"));
	}

	@Test
	public void testIsValid() {
		DataObject dataObject = new DataObject();
		dataObject.set("string", "hello world");
		dataObject.set("url", "http://www.example.com");
		dataObject.set("boolean", "true");
		dataObject.set("integer", "3");
		assertEquals(true, dataObject.isValid("string", "hello world"));
	}

	@Test
	public void testSimpleType() {
		class SimpleType extends DataObject {
			private static final long serialVersionUID = 1L;
		}
		SimpleType simpleType = new SimpleType();
		assertEquals(true, simpleType.isValid("string", "hello world"));
	}

	@Test
	public void testPutMultiple() {
		DataObject dataObject = makeCompanionDataObject();

		ArrayList<String> list = dataObject.<ArrayList<String>> get("stringList");
		assertEquals("Hello", list.get(0));
		assertEquals("World", list.get(1));

		dataObject.put("stringList", "from Java");

		list = dataObject.<ArrayList<String>> get("stringList");
		assertEquals("Hello", list.get(0));
		assertEquals("World", list.get(1));
		assertEquals("from Java", list.get(2));
	}

	@Test
	public void testEmbeddedList() {

		DataObject dataObject = makeCompanionDataObject();
		JSONArray jsonArray = new JSONArray();
		jsonArray.put("eins");
		jsonArray.put("zwei");
		dataObject.set("stringList", jsonArray.toString());

		dataObject.get("stringList", 1);
	}

	@Test
	public void testAttributeAccess() {
		for (String attribute : Schema.getAttributes(new DataObject())) {
			System.out.println(attribute + " is " + (Schema.isTrivialType(attribute) ? "trivial" : "non-trivial"));
		}
	}
}
