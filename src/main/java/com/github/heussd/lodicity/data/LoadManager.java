package com.github.heussd.lodicity.data;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.heussd.lodicity.store.Warehouse;

public class LoadManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(LoadManager.class);

	private LinkedHashMap<ArrayList<DataSource>, ArrayList<Loader>> pipeline = new LinkedHashMap<>();

	private Warehouse warehouse;

	private TimeZone tz;

	private SimpleDateFormat df;

	public LoadManager(Warehouse warehouse) {
		this.warehouse = warehouse;

		tz = TimeZone.getTimeZone("UTC");
		df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
		df.setTimeZone(tz);
	}

	public void register(Loader... loaders) {
		ArrayList<Loader> loadersList = new ArrayList<>();
		ArrayList<DataSource> dataSourceList = new ArrayList<>();

		Arrays.asList(loaders).forEach(loader -> {
			loadersList.add(loader);
			dataSourceList.addAll(loader.getDependentDataSources());
		});

		pipeline.put(dataSourceList, loadersList);
	}

	public void loadAll() {
		LOGGER.info("Load was triggered");

		pipeline.forEach((dataSources, loaders) -> {
			LOGGER.info("Load chain {}", loaders);

			boolean loadRequired = false;

			if (dataSources.size() != 0) {
				for (DataSource dataSource : dataSources) {
					MetaData metaData = warehouse.getMetaData(dataSource.getIdentifer());

					LOGGER.info("DataSource {}, last successful load {}", dataSource, metaData.lastSuccessData);
					String currentToken = dataSource.getCurrentnessToken();
					String storedToken = metaData.currentnessToken;
					LOGGER.debug("Currentness token is {}, stored one is {}", currentToken, storedToken);

					if (!currentToken.equals(storedToken)) {
						loadRequired = true;
					}
				}
			} else {
				LOGGER.warn("No datasources specified, asserting full load is required");
				loadRequired = true;
			}

			LOGGER.info("Load of this chain is {}required", (loadRequired ? "" : "not "));
			if (loadRequired) {
				loaders.forEach(loader -> {
					LOGGER.info("Instructing {} to load", loader.getClass().getSimpleName());
					loader.loadInto(warehouse);
				});

				dataSources.forEach(dataSource -> {
					LOGGER.info("DataSource {}, succesfully loaded", dataSource);

					MetaData metaData = warehouse.getMetaData(dataSource.getIdentifer());
					metaData.currentnessToken = dataSource.getCurrentnessToken();
					metaData.dataSourceIdentifier = dataSource.getIdentifer();
					metaData.lastSuccessData = df.format(new Date());
					warehouse.persistMetaData(metaData);
				});
			}
		});

	}
}
