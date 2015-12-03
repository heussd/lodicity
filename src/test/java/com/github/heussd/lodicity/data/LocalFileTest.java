package com.github.heussd.lodicity.data;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import org.junit.Test;

public class LocalFileTest {

	@Test
	public void testLocalFile() throws URISyntaxException {
		new PackagedResource("lodicity.schema.xlsx");
	}

	@Test
	public void testLocalFileString() {
		new LocalFileDataSource(".");
	}

	@Test(expected = RuntimeException.class)
	public void testLocalFileInvalid() throws Exception {
		new PackagedResource("IDONOTEXIST");
	}

	@Test(expected = RuntimeException.class)
	public void testLocalFileStringInvalid() {
		new LocalFileDataSource("IDONOTEXIST");
	}
	
	
	
	public void testCurrentnessToken() {
		String token = (new DataSource() {
			
			@Override
			public List<File> getLocalFiles() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getIdentifer() {
				// TODO Auto-generated method stub
				return null;
			}
		}.getCurrentnessToken());
		System.out.println(token);
		assertNotNull(token);
	}
}
