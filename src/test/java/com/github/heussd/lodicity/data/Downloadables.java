package com.github.heussd.lodicity.data;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

public class Downloadables {

	@Test
	public void testDownloadableDataSource() throws Exception {
		DownloadableDataSource downloadableDataSource = new DownloadableDataSource("http://example.com/index.html");
		System.out.println(downloadableDataSource);

		downloadableDataSource.getLocalFiles().forEach(file -> {
			assertTrue("To-be-downloaded file does not exist", file.exists());
		});
	}

	@Test
	public void testCKAN() throws Exception {
		CKANDataSource ckanDataSource = new CKANDataSource("https://datahub.io/dataset/mestske-obvody-v-brne");

		System.out.println(ckanDataSource);

		ckanDataSource.getLocalFiles().forEach(file -> {
			System.out.println(file);
		});
	}
}
