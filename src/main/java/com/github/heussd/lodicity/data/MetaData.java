package com.github.heussd.lodicity.data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class MetaData {

	// Required by Hibernate
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	long id;

	public MetaData() {

	}

	public MetaData(String identifier) {
		dataSourceIdentifier = identifier;
	}

	String currentnessToken;

	String dataSourceIdentifier;

	String lastSuccessData;
}
