import org.junit.Test;

import com.github.heussd.lodicity.data.LoadManager;
import com.github.heussd.lodicity.data.Loader;
import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.store.Filter;
import com.github.heussd.lodicity.store.Warehouse;

public class CompleteTestCase {

	@Test
	public void testCompleteRun() {
		final String field = "string";
		Warehouse warehouse = new Warehouse(true, DataObject.class);

		LoadManager loadManager = new LoadManager(warehouse);

		loadManager.register(new Loader() {
			@Override
			public void loadInto(Warehouse warehouse) {

				for (int i = 0; i <= 10; i++) {
					DataObject dataObject = new DataObject();

					// Randomly insert a selection criteria
					if (Math.round(Math.random() * 10) % 2 == 0) {
						dataObject.set(field, "please find me");
					} else {
						dataObject.set(field, "meh, dont find me");
					}
					warehouse.persist(dataObject);
				}
			}
		});

		loadManager.loadAll();

		Filter filter = new Filter(DataObject.class);
		warehouse.query(filter.eq(field, "please find me")).forEach(dataObject -> {
			System.out.println("Oh look, i found it: "+ dataObject);
		});
	}

}
