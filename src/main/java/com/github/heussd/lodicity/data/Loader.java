package com.github.heussd.lodicity.data;

import java.util.Arrays;
import java.util.List;

import com.github.heussd.lodicity.store.Warehouse;

public abstract class Loader {

	private List<DataSource> dataSources;

	public Loader(DataSource... dataSources) {
		this.dataSources = Arrays.asList(dataSources);
	}

	public List<DataSource> getDependentDataSources() {
		return dataSources;
	}

	public abstract void loadInto(Warehouse warehouse);

}
