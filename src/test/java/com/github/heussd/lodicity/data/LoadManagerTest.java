package com.github.heussd.lodicity.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.store.Warehouse;
import com.github.heussd.lodicity.store.WarehouseTest;

public class LoadManagerTest {

	class DoNothing extends Loader {

		public DoNothing(DataSource fileDataSource) {
			super(fileDataSource);
		}

		@Override
		public void loadInto(Warehouse warehouse) {
		}

	}

	class SimpleDataObjectLoader extends Loader {

		public SimpleDataObjectLoader(DataSource fileDataSource, DataSource fileDataSource2) {
			super(fileDataSource, fileDataSource2);
		}

		@Override
		public void loadInto(Warehouse warehouse) {
			warehouse.persist(WarehouseTest.makeCompanionDataObject());
		}
	}

	DataSource fileDataSource = new PackagedResource("lodicity.schema.xlsx");

	@Test
	public void testLoadScenario1() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		Loader doNothing = new DoNothing(fileDataSource);
		Loader simpleDataObjectLoader = new SimpleDataObjectLoader(fileDataSource, fileDataSource);

		LoadManager loadManager = new LoadManager(warehouse);
		loadManager.register(doNothing, simpleDataObjectLoader);
		loadManager.loadAll();

		assertEquals("DataSource has no metadata entry", fileDataSource.getCurrentnessToken(),
				warehouse.getMetaData(fileDataSource.getIdentifer()).currentnessToken);

		assertEquals("Loader was not executed (?)", new Long(1), warehouse.count(DataObject.class));

		warehouse.close();

		warehouse = new Warehouse(DataObject.class);
		loadManager = new LoadManager(warehouse);
		loadManager.register(new DoNothing(fileDataSource), new SimpleDataObjectLoader(fileDataSource, fileDataSource));
		loadManager.loadAll();

		assertEquals("Loader was executed wrongly (more than once?)", new Long(1), warehouse.count(DataObject.class));
		warehouse.close();

	}
}
