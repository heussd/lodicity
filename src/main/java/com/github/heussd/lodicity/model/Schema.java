package com.github.heussd.lodicity.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.print.attribute.standard.MediaSize.Other;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.github.heussd.lodicity.store.Warehouse;

/**
 * Offers a number of convenience methods to access the Schema for {@link DataObject} or to validate {@link DataObject} instances.
 * 
 * @author Timm Heuss, Hochschule Darmstadt - University of Applied Sciences, 2013
 */
public class Schema {

	private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
	private final static String SCHEMA_NAME = "lodicity.schema.xlsx";

	private final static Schema INSTANCE;

	static {
		try {
			INSTANCE = new Schema(new File(Schema.class.getResource("/" + SCHEMA_NAME).getFile()));
		} catch (Exception e) {
			throw new RuntimeException("Cannot load Schema", e);
		}
	}

	public static Schema getInstance() {
		return INSTANCE;
	}

	/**
	 * <b>How to use</b>: The schema is implemented with three nested {@link HashMap}s. Their keys have the following hierarchy:<br>
	 * <code>type -> attribute -> {@link SchemaProperty} -> property value</code> <br>
	 * for example:<br/>
	 * <code>Resource -> creator -> SchemaProperty.IS_LIST_TYPE -> true</code>
	 */
	private Map<String, HashMap<String, HashMap<SchemaProperty, Object>>> schemaModel;

	private Schema(File file) {
		try {
			schemaModel = new HashMap<>();

			Workbook workbook = null;
			try (InputStream inputStream = new FileInputStream(file)) {
				workbook = WorkbookFactory.create(inputStream);
			}

			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				Sheet sheet = workbook.getSheetAt(i);

				// Make sure the selected sheet has the right structure
				Row firstRow = sheet.getRow(0);
				if (!firstRow.getCell(SchemaProperty.ATTRIBUTE_NAME.cellIndex).toString().equals("Attribute"))
					continue;

				assert firstRow.getCell(SchemaProperty.CARDINALITY.cellIndex).toString().equals("Cardinality") : "Invalid sheet structure in sheet \""
						+ sheet.getSheetName() + "\": Cardinality not found";

				assert firstRow.getCell(SchemaProperty.DATATYPE.cellIndex).toString().equals("Datatype") : "Invalid sheet structure in sheet \""
						+ sheet.getSheetName() + "\": Datatype not found";

				assert firstRow.getCell(SchemaProperty.APPLICATION.cellIndex).toString().equals("Application") : "Invalid sheet structure in sheet \""
						+ sheet.getSheetName() + "\": Application not found";

				// Produce a random access structure of the selected sheet
				HashMap<String, HashMap<SchemaProperty, Object>> attributeDefintion = new HashMap<>();
				for (int rowNumber = 1; rowNumber <= sheet.getLastRowNum(); rowNumber++) {
					Row attributeRow = sheet.getRow(rowNumber);

					HashMap<SchemaProperty, Object> attributes = new HashMap<>();

					// There might be empty rows in the schema, skip them here
					if (attributeRow == null)
						continue;

					// The getStringCellValue method explicitly validates a cell
					// to be of type String - this is pretty much of use here...
					String attribute = attributeRow.getCell(SchemaProperty.ATTRIBUTE_NAME.cellIndex) != null
							? attributeRow.getCell(SchemaProperty.ATTRIBUTE_NAME.cellIndex).getStringCellValue() : null;

					if (attribute == null)
						continue;

					// Read all properties from excel
					for (SchemaProperty schemaProperty : SchemaProperty.values()) {
						// Only consider properties with a valid cell index
						if (schemaProperty.cellIndex > 0) {
							attributes.put(schemaProperty,
									attributeRow.getCell(schemaProperty.cellIndex) != null ? attributeRow.getCell(schemaProperty.cellIndex).toString() : null);
						}
					}

					// Interpret cardinality, create field IS_LIST_TYPE
					if (attributes.get(SchemaProperty.CARDINALITY) != null) {
						String cardinality = (String) attributes.get(SchemaProperty.CARDINALITY);
						assert (cardinality.length() == 3 || cardinality.length() == 4) : "Invalid cardinality: \"" + cardinality + "\"";

						attributes.put(SchemaProperty.IS_LIST_TYPE, cardinality.substring(cardinality.length() - 1, cardinality.length()).equals("*"));
					}

					// Interpret enum entries, create field VALUES
					if (attributes.get(SchemaProperty.DATATYPE) != null) {
						String datatype = (String) attributes.get(SchemaProperty.DATATYPE);

						if (datatype.length() > 4 && datatype.substring(0, 4).equals("enum")) {
							Collection<String> values = new ArrayList<>();
							Collections.addAll(values, (datatype.substring(5, datatype.length() - 1)).split(", "));
							attributes.put(SchemaProperty.VALUES, values);

							// Change DATATYPE
							attributes.put(SchemaProperty.DATATYPE, "String");

						}
					}
					attributeDefintion.put(attribute, attributes);
				}
				schemaModel.put(sheet.getSheetName(), attributeDefintion);
			}
		} catch (Throwable t) {
			throw new RuntimeException("Cannot initialize schema", t);
		}
	}

	/**
	 * Returns all valid attribute-keys defined for the given {@link DataObject} -specialization.
	 * 
	 * @param class1
	 * @return
	 */
	public static Set<String> getAttributes(DataObject dataObject) {
		return getInstance().schemaModel.get(dataObject.getClass().getSimpleName()).keySet();
	}

	/**
	 * Returns all valid attribute-keys defined for the given {@link DataObject} -specialization.
	 * 
	 * @param class1
	 * @return
	 */
	public static Set<String> getAttributes(String dataObject) {
		return getInstance().schemaModel.get(dataObject).keySet();
	}

	/**
	 * Returns all valid attribute-keys for the given application defined for the given {@link DataObject} -specialization.
	 * 
	 * @param class1
	 * @return
	 */
	public static Set<String> getAttributes(DataObject dataObject, String application) {
		HashMap<String, HashMap<SchemaProperty, Object>> dataObjectAttributes = getInstance().schemaModel.get(dataObject.getClass().getSimpleName());
		Set<String> applicationKeySet = new HashSet<>();

		// iterate through all attributes of the dataObject
		for (Entry<String, HashMap<SchemaProperty, Object>> stringHashMapEntry : dataObjectAttributes.entrySet()) {
			Entry<String, HashMap<SchemaProperty, Object>> pairs = (Entry) stringHashMapEntry;

			// get SchemaPropertys
			HashMap<SchemaProperty, Object> schemaPropertys = pairs.getValue();

			// get application
			String applicationSchemaProperty = (String) schemaPropertys.get(SchemaProperty.APPLICATION);

			// schema property for the given application == null means for both (museum and library)
			if (applicationSchemaProperty == null || applicationSchemaProperty.equals(application)) {
				applicationKeySet.add((String) pairs.getKey());
			}

			// it.remove(); // avoids a ConcurrentModificationException
		}
		return applicationKeySet;
	}

	/**
	 * Returns all properties of a given attribute of a given type.
	 * 
	 * @param dataObject
	 * @param attribute
	 * @return
	 */
	public static Map<SchemaProperty, Object> getAttributeDefinition(DataObject dataObject, String attribute) {
		assert getInstance().schemaModel.containsKey(dataObject.getClass().getSimpleName()) : "Schema definition not found for type \""
				+ dataObject.getClass().getSimpleName() + "\"";

		Map<SchemaProperty, Object> attributeDefinition = getInstance().schemaModel.get(dataObject.getClass().getSimpleName()).get(attribute);

		assert attributeDefinition != null : "No attribute definition found for attribute \"" + attribute + "\" in type \""
				+ dataObject.getClass().getSimpleName() + "\"";

		return attributeDefinition;
	}

	/**
	 * Indicates if a given attribute of a given {@link DataObject} is a List-type.
	 * 
	 * @param dataObject
	 * @param attribute
	 * @return
	 */
	public static boolean isListType(DataObject dataObject, String attribute) {
		Map<SchemaProperty, Object> attributeDefinition = getAttributeDefinition(dataObject, attribute);

		return attributeDefinition.get(SchemaProperty.IS_LIST_TYPE) != null && (boolean) attributeDefinition.get(SchemaProperty.IS_LIST_TYPE);
	}

	/**
	 * Indicates if a given attribute of a given {@link DataObject} is a List-type.
	 * 
	 * @param dataObject
	 * @param attribute
	 * @return
	 */
	public static boolean isListType(Class<? extends DataObject> dataObjectClass, String attribute) {
		assert dataObjectClass != null : "DataObjectClass is null";
		assert attribute != null : "Attribute is null";

		Map<SchemaProperty, Object> attributeDefinition = getInstance().schemaModel.get(dataObjectClass.getSimpleName()).get(attribute);

		assert attributeDefinition != null : "no attribute definition found for attribute " + attribute + " in type " + dataObjectClass.getSimpleName();
		return attributeDefinition.get(SchemaProperty.IS_LIST_TYPE) != null && (boolean) attributeDefinition.get(SchemaProperty.IS_LIST_TYPE);
	}

	public static boolean isMandatory(DataObject dataObject, String attribute) {
		Map<SchemaProperty, Object> attributeDefinition = getAttributeDefinition(dataObject, attribute);

		return attributeDefinition.get(SchemaProperty.CARDINALITY) != null && attributeDefinition.get(SchemaProperty.CARDINALITY).toString().startsWith("1");
	}

	public static boolean isOptional(DataObject dataObject, String attribute) {
		Map<SchemaProperty, Object> attributeDefinition = getAttributeDefinition(dataObject, attribute);

		return attributeDefinition.get(SchemaProperty.CARDINALITY) != null && attributeDefinition.get(SchemaProperty.CARDINALITY).toString().startsWith("0");
	}

	/**
	 * @deprecated Use {@link #getDataType(DataObject, String)} instead.
	 */
	public static String getSchemaDefinedDataType(DataObject dataObject, String attribute) {
		return getInstance().schemaModel.get(dataObject.getClass().getSimpleName()).get(attribute).get(SchemaProperty.DATATYPE).toString();
	}

	public static boolean isValid(DataObject dataObject, String attribute, Object value) {
		// Special logic applies to this attribute:
		if (attribute.equals("_class_") && dataObject.getClass().getSimpleName().equals(value))
			return true;

		try {
			Map<SchemaProperty, Object> attributeDefinition = Schema.getAttributeDefinition(dataObject, attribute);

			if (value == null) {
				/*
				 * Does the schema allow the value to be empty or null?
				 */
				if (Schema.isOptional(dataObject, attribute))
					return true;

				if (Schema.isMandatory(dataObject, attribute)) {
					return false;
				}

				// Value is null, but not cardinality is given -> OK
				return true;
			}

			String schemaDefinedDataType = Schema.getDataType(dataObject, attribute);

			if (Schema.isListType(dataObject, attribute) || Schema.isPairType(dataObject, attribute)) {
				// List types go in here...
				// Special validation case for Pair* types: Validate
				// them as if they are lists

				assert value instanceof Collection<?> : "Invalid attribute type \"" + value.getClass().getSimpleName() + "\", expected type was \"Collection\"";

				if (((Collection) value).size() == 0 && isMandatory(dataObject, attribute))
					throw new RuntimeException("Mendatory list has zero elements");

				for (Object innerValue : (Collection<?>) value) {

					if (schemaDefinedDataType != null && schemaDefinedDataType.startsWith("Pair")) {
						// Special validation case for Pair* types: Validate
						// them as if they are lists
						assert (value.getClass().getSimpleName()
								.equals(ArrayList.class.getSimpleName())) : "Attribute of type \"Pair\" does not contain an ArrayList, but \""
										+ value.getClass().getSimpleName() + "\"";
					} else {
						assert schemaDefinedDataType == null
								|| innerValue.getClass().getSimpleName().equals(schemaDefinedDataType) : "Invalid list entity type \""
										+ innerValue.getClass().getSimpleName() + "\", expected type was \"" + schemaDefinedDataType + "\"";

						if (attributeDefinition.containsKey(SchemaProperty.VALUES)) {
							// Schema defines the exact inner-list values that
							// are allowed for this attribute
							assert ((ArrayList<String>) attributeDefinition.get(SchemaProperty.VALUES))
									.contains(innerValue) : "Invalid inner-list attribute value \"" + innerValue + "\", expected any of "
											+ attributeDefinition.get(SchemaProperty.VALUES);
						}
					}
				}
			} else {
				// Non-List-type attribute, make sure it is of the
				// schema-defined type
				if (schemaDefinedDataType != null && !schemaDefinedDataType.equals("null")) {
					// Most likely because of the JSON framework, number types
					// are sometimes mixed up, e.g. an Integer is read as Long.
					// We don't seem to have influence on that, so we do not
					// verify their current data type, but if they are castable
					// into the Schema-defined type.
					try {
						switch (schemaDefinedDataType) {
						case "Float":
							new Float(value + "");
							break;
						case "Integer":
							new Integer(value + "");
							break;
						case "URL":
							new URL(value.toString());
							break;
						case "Boolean":
							String v = value.toString();
							if (!(v.equals("true") || v.equals("false"))) {
								throw new ClassCastException("Cannot convert the value " + v + " to Boolean");
							}
							break;
						default:
							// General purpose datatype validation
							assert value.getClass().getSimpleName().equals(schemaDefinedDataType) : "Invalid attribute type \""
									+ value.getClass().getSimpleName() + "\", expected type was \"" + schemaDefinedDataType + "\"";
							break;
						}
					} catch (Exception e) {
						throw new RuntimeException("Cannot cast type \"" + value.getClass().getSimpleName() + "\" to \"" + schemaDefinedDataType + "\"", e);
					}
				}

				if (attributeDefinition.containsKey(SchemaProperty.VALUES)) {
					// Schema defines the exact values that are allowed for this
					// attribute
					assert ((ArrayList<String>) attributeDefinition.get(SchemaProperty.VALUES)).contains(value) : "Invalid attribute value \"" + value
							+ "\", expected any of " + attributeDefinition.get(SchemaProperty.VALUES);
				}

			}

			return true;
		} catch (Throwable e) {
			throw new RuntimeException(
					"Validation failed for attribute \"" + attribute + "\", value \"" + value + "\" in type \"" + dataObject.getClass().getSimpleName() + "\"",
					e);
		}

	}

	/**
	 * Validates the given {@link DataObject} against the schema. In case of validation failures, a {@link RuntimeException} is thrown.
	 */
	public static void validate(DataObject dataObject) {
		assert dataObject != null : "Cannot validate a null object";

		try {
			for (String attribute : getAttributes(dataObject)) {
				// Make a validated get
				dataObject.get(attribute);
			}
		} catch (Exception e) {
			throw new RuntimeException(
					"Schema validation failed for DataObject of type \"" + dataObject.getClass().getSimpleName() + "\". Invalid DataObject is " + dataObject,
					e);
		}
	}

	public static boolean isPairType(DataObject dataObject, String attribute) {
		String schemaDefinedDataType = Schema.getDataType(dataObject, attribute);
		if (schemaDefinedDataType != null)
			return schemaDefinedDataType.startsWith("Pair");

		return false;
	}

	public static String getDataType(DataObject dataObject, String attribute) {
		if (attribute.equals("_class_")) {
			return "String";
		}

		try {
			return (String) getInstance().schemaModel.get(dataObject.getClass().getSimpleName()).get(attribute).get(SchemaProperty.DATATYPE);
		} catch (Exception e) {
			throw new RuntimeException("Cannot read data type of attribute " + attribute + " of type " + dataObject.getClass().getSimpleName());
		}
	}

	public static String getDataType(Class<? extends DataObject> dataObjectClass, String attribute) {
		return (String) getInstance().schemaModel.get(dataObjectClass.getSimpleName()).get(attribute).get(SchemaProperty.DATATYPE);
	}

	public static String generateHibernateMapping(Class<? extends DataObject> dataObjectClass)
			throws UnsupportedEncodingException, TransformerException, ParserConfigurationException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = dbf.newDocumentBuilder();
		Document doc = builder.newDocument();

		// create the root element node
		Element root = doc.createElement("hibernate-mapping");
		doc.appendChild(root);

		Element entity = doc.createElement("class");
		root.appendChild(entity);

		entity.setAttribute("entity-name", dataObjectClass.getCanonicalName() == null ? dataObjectClass.getSimpleName() : dataObjectClass.getCanonicalName());
		entity.setAttribute("table", dataObjectClass.getSimpleName().toUpperCase());

		// Meta section
		Element meta = doc.createElement("meta");
		meta.setAttribute("attribute", "class-description");
		meta.insertBefore(doc.createTextNode("Hibernate mapping for the LODicity entity " + dataObjectClass.getSimpleName()), meta.getLastChild());
		entity.appendChild(meta);

		// Meta section
		// <id name="id" column="modelId" length="32" type="string">
		// <generator class="uuid.hex" />
		// </id>

		Element id = doc.createElement("id");
		id.setAttribute("name", "hibernateInternalId");
		id.setAttribute("type", "string");
		id.setAttribute("column", "HIBERNATEINTERNALID");
		Element generator = doc.createElement("generator");
		generator.setAttribute("class", "native");
		id.appendChild(generator);
		entity.appendChild(id);

		for (String attribute : getAttributes(dataObjectClass.getSimpleName())) {
			Element property = doc.createElement("property");
			property.setAttribute("name", attribute);
			property.setAttribute("column", attribute);
			property.setAttribute("index", "IDX_" + dataObjectClass.getSimpleName() + "_" + attribute);
			String type = getHibernateType(getDataType(dataObjectClass, attribute));
			property.setAttribute("type", type);
			entity.appendChild(property);
		}

		// create text for the node
		// itemElement.insertBefore(doc.createTextNode("text"), itemElement.getLastChild());

		Transformer tf = TransformerFactory.newInstance().newTransformer();
		tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		tf.setOutputProperty(OutputKeys.INDENT, "yes");
		Writer out = new StringWriter();
		tf.transform(new DOMSource(doc), new StreamResult(out));

		LOGGER.info("Produced Hibernate type definition for {} \n{}", dataObjectClass.getSimpleName(), out.toString());
		return out.toString();
	}

	public final static boolean isTrivialType(String type) {
		switch (type) {
		case "Enum":
		case "URL":
		case "Float":
		case "String":
		case "Integer":
		case "Boolean":
			return true;
		default:
			return false;
		}
	}

	private static String getHibernateType(String dataType) {
		// TODO Should be warned or something
		// assert dataType != null : "dataType is null";
		if (dataType == null)
			return "string";

		switch (dataType) {
		case "Enum":
		case "URL":
		case "String":
			return "string";
		case "Float":
			return "float";
		case "Integer":
			return "int";
		case "Boolean":
			return "boolean";
		default:
			// Most likely, we have to handle complex datatypes here.

			return "string";

		// throw new RuntimeException("Cannot assign a hibernate type for data type " + dataType);
		}
	}

}
