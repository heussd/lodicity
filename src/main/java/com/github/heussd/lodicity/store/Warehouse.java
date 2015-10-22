package com.github.heussd.lodicity.store;

import java.io.Closeable;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.apache.commons.io.IOUtils;
import org.hibernate.EntityMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.model.DataObjectIterable;
import com.github.heussd.lodicity.model.Schema;

public class Warehouse implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Warehouse.class);
	private SessionFactory factory;
	private Session session;

	public Warehouse() {
		this(false, DataObject.class);
	}

	@SafeVarargs
	public Warehouse(Class<? extends DataObject>... dataObjectClasses) {
		this(false, dataObjectClasses);
	}

	/**
	 * 
	 * @param clear
	 *            Remove all existing structures (true) or re-use existing
	 * @param dataObjectClasses
	 */
	@SafeVarargs
	public Warehouse(boolean clear, Class<? extends DataObject>... dataObjectClasses) {
		LOGGER.debug("T.H. LODicity Warehouse");
		java.util.logging.Logger.getLogger("org.hibernate").setLevel(Level.FINEST);
		try {
			Configuration configuration = new Configuration().configure();
			configuration.setProperty(Environment.DEFAULT_ENTITY_MODE, EntityMode.MAP.toString());
			configuration.setProperty(Environment.SHOW_SQL, "true");
			configuration.setInterceptor(new DataObjectInterceptor());

			if (clear) {
				LOGGER.warn("Clear-Flag set, will erase existing data structures");
				configuration.setProperty(Environment.HBM2DDL_AUTO, "create");
			}

			// For each given DataObject-Class, init a Hibernate mapping.
			for (Class<? extends DataObject> dataObjectClass : dataObjectClasses) {
				LOGGER.debug("Registering DataObject Type {}", dataObjectClass.getSimpleName());
				configuration.addInputStream(IOUtils.toInputStream(Schema.generateHibernateMapping(dataObjectClass), "UTF-8"));
			}
			factory = configuration.buildSessionFactory();
			session = factory.openSession();
		} catch (Throwable e) {
			throw new RuntimeException("Failed to create Warehouse", e);
		}
	}

	public void persist(DataObject... dataObjects) {
		assert session != null : "Session is null";
		assert dataObjects != null : "Given DataObject(s) are null";

		Transaction transaction = session.beginTransaction();
		for (DataObject dataObject : dataObjects) {
			session.save(dataObject);
		}
		transaction.commit();
	}

	@SuppressWarnings("unchecked")
	public Iterable<? extends DataObject> all(Class<? extends DataObject> dataObjectClass) {
		return new DataObjectIterable(dataObjectClass, session.createCriteria(dataObjectClass).list());
	}

	@SuppressWarnings("unchecked")
	public void forEach(Class<? extends DataObject> dataObjectClass, Consumer<? super DataObject> consumer) {
		new DataObjectIterable(dataObjectClass, session.createCriteria(dataObjectClass).list()).forEach(consumer);
	}

	@Override
	public void close() {
		assert session != null : "Session is null";
		assert session.isOpen() : "Session is not open";
		session.close();
	}

	public void persist(List<? extends DataObject> dataObjects) {
		assert session != null : "Session is null";
		assert dataObjects.size() != 0 : "No DataObject(s) given";

		Transaction transaction = session.beginTransaction();
		for (DataObject dataObject : dataObjects) {
			session.save(dataObject);
		}
		transaction.commit();
	}

}
