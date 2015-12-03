package com.github.heussd.lodicity.data;

import java.net.URISyntaxException;

import org.junit.Test;

public class LocalFileTest {

	
	@Test
	public void testLocalFile() throws URISyntaxException {
		new PackagedResource("lodicity.schema.xlsx");
	}
}
