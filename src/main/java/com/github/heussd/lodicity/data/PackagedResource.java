package com.github.heussd.lodicity.data;

public class PackagedResource extends LocalFileDataSource {

	public PackagedResource(String localFilePath) {
		super(PackagedResource.class.getResource("/" + localFilePath).getFile());
	}

}
