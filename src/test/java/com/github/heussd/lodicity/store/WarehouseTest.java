package com.github.heussd.lodicity.store;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.github.heussd.lodicity.model.DataObject;

public class WarehouseTest {

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
	public void testWriteAndRead() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		warehouse.persist(makeCompanionDataObject());

		warehouse.all(DataObject.class).forEach(dataObject -> {
			System.out.println(dataObject);

			assertEquals(COMPANION_STRING, dataObject.<String> get("string"));
			assertEquals(1F, dataObject.<String> get("float"));
		});

		warehouse.close();
	}

	@Test
	public void testSimpleTypePersistence() {
		SimpleType simpleType = new SimpleType();
		simpleType.set("string", "hello simple type");

		Warehouse warehouse = new Warehouse(true, DataObject.class, SimpleType.class);
		warehouse.persist(simpleType);

		warehouse.forEach(SimpleType.class, mySimpleType -> {
			assertEquals("SimpleType", mySimpleType.getClass().getSimpleName());
			assertEquals("hello simple type", mySimpleType.<String> get("string"));
		});

		warehouse.close();
	}

	@Test
	public void testListType() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		warehouse.persist(makeCompanionDataObject());

		warehouse.forEach(DataObject.class, dataObject -> {
			ArrayList<String> strings = dataObject.<ArrayList<String>> get("stringList");
			assertEquals("Hello", strings.get(0));
			assertEquals("World", strings.get(1));
		});

		warehouse.close();
	}

	@Test
	public void testEmbeddedType() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		DataObject dataObject = makeCompanionDataObject();
		SimpleType simpleType = new SimpleType();
		simpleType.put("string", "String of a Simple Type");
		dataObject.put("simpleType", simpleType);
		warehouse.persist(dataObject);

		warehouse.forEach(DataObject.class, d -> {
			SimpleType s = d.<SimpleType> get("simpleType");
			assertEquals("SimpleType", s.getClass().getSimpleName());
			assertEquals("String of a Simple Type", s.<String> get("string"));
		});

		warehouse.close();
	}

	@Test
	@Ignore
	public void testFullTextSearch() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		DataObject dataObject = makeCompanionDataObject();

		warehouse.persist(dataObject);
//		warehouse.search(DataObject.class, "string", "nasty");
	}

}
