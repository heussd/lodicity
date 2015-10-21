package com.github.heussd.lodicity.model;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

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
	public void testEmbeddedSimpleType() {
		
	}
}
