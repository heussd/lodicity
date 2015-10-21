package com.github.heussd.lodicity.model;

/**
 * Represents a certain property of the schema definition for an attribute, including its cell index in the schema file.
 */
public enum SchemaProperty {
	ATTRIBUTE_NAME(0),
	DATATYPE(2),
	CARDINALITY(3),
	APPLICATION(4),
	VALUES(-1),
	IS_LIST_TYPE(-1);

	public final int cellIndex;

	private SchemaProperty(int index) {
		this.cellIndex = index;
	}
}