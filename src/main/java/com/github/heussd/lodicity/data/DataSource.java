package com.github.heussd.lodicity.data;

import java.io.File;
import java.util.List;
import java.util.Random;

public abstract class DataSource {

	/**
	 * Generates or retrieves a special string token that represents the DataSource's currentness. This token will be used to determine if the DataSoruce has changed and if a load
	 * should be triggered.
	 * 
	 * @return
	 */
	public String getCurrentnessToken() {
		return new Random().toString();
	}

	public abstract String getIdentifer();

	public abstract List<File> getLocalFiles();
	
	
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
