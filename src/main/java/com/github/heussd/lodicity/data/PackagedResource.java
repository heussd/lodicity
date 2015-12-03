package com.github.heussd.lodicity.data;

import java.net.URISyntaxException;

public class PackagedResource extends LocalFileDataSource {

	public PackagedResource(String localFilePath) throws URISyntaxException {
		super(PackagedResource.class.getResource("/" + localFilePath));
	}

}
