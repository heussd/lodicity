package com.github.heussd.lodicity.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.criterion.Restrictions;
import org.junit.Ignore;
import org.junit.Test;

import com.github.heussd.lodicity.data.MetaData;
import com.github.heussd.lodicity.model.DataObject;

public class WarehouseTest {

	private final static String COMPANION_STRING = "Hello this is a string with nasty characters äöü!";

	public final static DataObject makeCompanionDataObject() {
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
	public void testObjectManipulation() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		DataObject dataObject = makeCompanionDataObject();

		warehouse.persist(dataObject);

		int counter = 0;
		for (DataObject d : warehouse.all(DataObject.class)) {
			System.out.println("item " + ++counter + ": " + d.<String> get("string"));
		}

		System.out.println("Warehouse has " + counter + " items");
		assertEquals(1, counter);

		// dataObject.set("string", "Changed value");
		// warehouse.persist(dataObject);

		warehouse.forEach(DataObject.class, d -> {
			d.set("string", "Changed value");
			warehouse.update(d);
		});
		//

		counter = 0;
		for (DataObject d : warehouse.all(DataObject.class)) {
			System.out.println("item " + ++counter + ": " + d.<String> get("string"));
		}

		System.out.println("Warehouse has " + counter + " items");
		assertEquals(1, counter);

		warehouse.close();
	}

	@Test
	public void testFiltered() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		DataObject dataObject = makeCompanionDataObject();

		DataObject dataObject2 = makeCompanionDataObject();
		dataObject2.set("string", "hello filter please match me");

		warehouse.persist(dataObject, dataObject2);

		Filter filter = new Filter(DataObject.class);

		warehouse.query(filter.eq("string", "please")).forEach(d -> {
			assertTrue("This should not exact match.", false);
		});

		filter = new Filter(DataObject.class);

		warehouse.query(filter.ilike("string", "please"), filter.ilike("string", "filter")).forEach(d -> {
			assertEquals("Expected working ilike did not work", "hello filter please match me", d.<String> get("string"));
		});

		//
		// warehouse.stream().filter(u -> u.age > 30 || u.lastName.startsWith("S")).collect(Collectors.toList());
		warehouse.close();
	}

	@Test
	public void testCount() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		DataObject dataObject = makeCompanionDataObject();
		warehouse.persist(dataObject);

		Filter filter = new Filter(DataObject.class);
		assertEquals("Counting for just added DataObject", new Long("1"), warehouse.count(filter.eq("string", COMPANION_STRING)));
		warehouse.close();
	}

	@Test
	public void testCountedOrQuery() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		DataObject dataObject = makeCompanionDataObject();

		DataObject dataObject2 = makeCompanionDataObject();
		dataObject2.set("string", "hello filter please match me");

		warehouse.persist(dataObject, dataObject2);

		Filter filter = new Filter(DataObject.class);

		assertEquals("Counting for just added DataObject", new Long("2"),
				warehouse.count(filter.or(filter.eq("string", COMPANION_STRING), filter.eq("string", "hello filter please match me"))));
		warehouse.close();
	}

	@Test
	public void testMetadata() {
		MetaData metaData = new MetaData("test");
		Warehouse warehouse = new Warehouse();
		warehouse.persistMetaData(metaData);
		warehouse.close();
	}

	@Test
	public void testTransaction() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);
		warehouse.openTransaction();

		for (int i = 0; i < 10; i++) {
			DataObject dataObject = new DataObject();
			dataObject.set("string", "TEST");
			warehouse.massUpdate(dataObject);
		}

		warehouse.commit();

		assertEquals(new Long(10), warehouse.count(DataObject.class));
		warehouse.close();
	}

	@Test(expected = AssertionError.class)
	public void testEmptyQuery() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		warehouse.query();
		warehouse.close();
	}
	
	@Test(expected = AssertionError.class)
	public void testEmptyCount() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		warehouse.count();
		warehouse.close();
	}
	
	@Test(expected = AssertionError.class)
	public void testInvalidMassUpdate1() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		warehouse.massUpdate(makeCompanionDataObject());
	}
	
	@Test(expected = AssertionError.class)
	public void testInvalidMassUpdate2() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		warehouse.massUpdate(null);
	}
}
