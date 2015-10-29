package com.github.heussd.lodicity.data;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class LocalFileDataSource extends DataSource {

	private File file;

	public LocalFileDataSource(String localFilePath) {
		this.file = new File(localFilePath);

		if (!file.exists())
			throw new RuntimeException("File does not exist \"" + file + "\"");
	}

	@Override
	public String getCurrentnessToken() {
		try {
			return Files.getLastModifiedTime(file.toPath()).toString();
		} catch (Exception e) {
			throw new RuntimeException("Could not determine last modifcation time", e);
		}
	}

	@Override
	public List<File> getLocalFiles() {
		return new ArrayList<>();
	}

	@Override
	public String getIdentifer() {
		return file.toString();
	}

}
