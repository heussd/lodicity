package com.github.heussd.lodicity.store;

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

import com.github.heussd.lodicity.model.DataObject;

@Ignore
public class WarehouseAdvancedFeatures {

	@Test
	public void testIncremental() {
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		DataObject dataObject = WarehouseTest.makeCompanionDataObject();

		warehouse.persist(dataObject);

		dataObject = WarehouseTest.makeCompanionDataObject();

		warehouse.persist(dataObject);

		assertEquals(new Long(1), warehouse.count(DataObject.class));
		
		warehouse.close();
	}
}
