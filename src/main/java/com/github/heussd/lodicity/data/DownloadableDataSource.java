package com.github.heussd.lodicity.data;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.axet.wget.WGet;
import com.github.axet.wget.info.DownloadInfo;

public class DownloadableDataSource extends DataSource {
	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadableDataSource.class);

	protected URL url;

	public DownloadableDataSource(URL url) {
		this.url = url;
	}

	public DownloadableDataSource(String url) throws MalformedURLException {
		this(new URL(url));
	}

	@Override
	public String getIdentifer() {
		return url.toString();
	}

	@Override
	public List<File> getLocalFiles() {
		LOGGER.info("Downloading {}", url);
		File file = new File(new File(url.getFile()).getName());
		download(url, file);
		return Arrays.asList(new File[] { file });
	}

	protected void download(URL url, File file) {
		DownloadInfo info = new DownloadInfo(url);
		info.extract();
		WGet wGet = new WGet(info, file);
		wGet.download();
	}

}
