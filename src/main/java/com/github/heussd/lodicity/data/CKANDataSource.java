package com.github.heussd.lodicity.data;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CKANDataSource extends DownloadableDataSource {
	private static final Logger LOGGER = LoggerFactory.getLogger(CKANDataSource.class);

	public CKANDataSource(String url) throws MalformedURLException {
		super(url);
	}

	@Override
	public List<File> getLocalFiles() {
		try {
			String dataPortal = (url.getHost()).toString();
			String datasetId = (new File(url.getFile()).getName()).toString();

			// http://datahub.io/api/3/action/package_show?id=adur_district_spending
			LOGGER.info("Constructing CKAN API Version 3 Query for \"{}\"", dataPortal);
			URL ckanRequest = new URL("http://" + dataPortal + "/api/3/action/package_show?id=" + datasetId);

			LOGGER.info("Requesting CKAN meta data for \"{}\"", datasetId);
			Scanner scanner = new Scanner(ckanRequest.openStream());
			String response = scanner.useDelimiter("\\Z").next();
			JSONObject json = new JSONObject(response);
			scanner.close();
			LOGGER.info("Parsing response...");

			if (!json.get("success").toString().equals("true"))
				throw new RuntimeException("Response indicated unsuccessful operation");

			ArrayList<File> files = new ArrayList<>();
			JSONArray resources = (JSONArray) (((JSONObject) json.get("result")).get("resources"));
			for (int i = 0; i < resources.length(); i++) {
				JSONObject resource = (JSONObject) resources.get(i);
				LOGGER.info("Retrieving {} resource {}", resource.get("format"), resource.get("url"));

				File file = new File((String) resource.get("id"));
				download(new URL((String) resource.get("url")), file);
				files.add(file);
			}
			return files;
		} catch (Exception e) {
			throw new RuntimeException("Cannot retrieve files from \"" + url + "\"", e);
		}
	}

}
