package com.github.heussd.lodicity.store;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

import org.apache.commons.io.IOUtils;
import org.hibernate.Criteria;
import org.hibernate.EntityMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Projections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.heussd.lodicity.data.MetaData;
import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.model.DataObjectIterable;
import com.github.heussd.lodicity.model.Schema;

public class Warehouse implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(Warehouse.class);
	private SessionFactory factory;
	private Session session;
	private Transaction transaction;
	private Query query;

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
			configuration.setProperty(Environment.SHOW_SQL, "false");
			configuration.setInterceptor(new DataObjectInterceptor());

			if (clear) {
				LOGGER.warn("Clear-Flag set, will erase existing data structures");
				configuration.setProperty(Environment.HBM2DDL_AUTO, "create");
			}

			for (Class<? extends DataObject> dataObjectClass : dataObjectClasses) {
				LOGGER.debug("Registering DataObject Type {}", dataObjectClass.getSimpleName());

				// Make a Hibernate mapping based on Schema information
				configuration.addInputStream(IOUtils.toInputStream(Schema.generateHibernateMapping(dataObjectClass), "UTF-8"));
			}

			// <property name="hibernate.search.model_mapping">
			// com.packtpub.hibernatesearch.util.SearchMappingFactory
			// </property>
			// LOGGER.info("Registering {}", DataObjectSearchMappingFactory.class.getCanonicalName());
			// configuration.setProperty(org.hibernate.search.cfg.Environment.MODEL_MAPPING, DataObjectSearchMappingFactory.class.getCanonicalName());

			factory = configuration.buildSessionFactory();
			session = factory.openSession();

			Transaction transaction = session.beginTransaction();
			session.createSQLQuery("PRAGMA journal_mode=WAL");

			// this.query = session.createQuery("FROM DataObject dataObject WHERE dataObject.string = :string");
			transaction.commit();

			// FullTextSession fullTextSession = Search.getFullTextSession(session);
			// System.out.println(fullTextSession.getSearchFactory().getIndexedTypes().size());
			// for (Class c : fullTextSession.getSearchFactory().getIndexedTypes()) {
			// System.out.println(c);
			// }

		} catch (Throwable e) {
			throw new RuntimeException("Failed to create Warehouse", e);
		}
	}

	public void persist(DataObject... dataObjects) {
		persist(Arrays.asList(dataObjects));
	}

	@SuppressWarnings("unchecked")
	public Iterable<? extends DataObject> all(Class<? extends DataObject> dataObjectClass) {
		return new DataObjectIterable(dataObjectClass, session.createCriteria(dataObjectClass).list());
	}

	public void forEach(Class<? extends DataObject> dataObjectClass, Consumer<? super DataObject> consumer) {
		all(dataObjectClass).forEach(consumer);
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
		LOGGER.info("Persisting {} items...", dataObjects.size());

		// session.get(DataObject.class, );
		// // has?
		// {long start = System.nanoTime();
		// count(DataObject.class, Restrictions.eq("string", dataObjects.get(0).<String>get("string")));
		// long end = System.nanoTime();
		// System.out.println("Counting took " + (end-start));
		// }

		long start = System.nanoTime();
		Transaction transaction = session.beginTransaction();
		for (DataObject dataObject : dataObjects) {

			session.saveOrUpdate(dataObject);
		}
		long end = System.nanoTime();
		LOGGER.info("Persisted items in {} milliseconds", (end - start) / 100000);
		transaction.commit();
	}

	public void update(DataObject dataObject) {
		assert session != null : "Session is null";
		assert dataObject != null : "No DataObject given";

		Transaction transaction = session.beginTransaction();
		session.merge(dataObject);
		transaction.commit();
	}

	public Iterable<? extends DataObject> query(Class<? extends DataObject> dataObjectClass) {
		return query(new Filter(dataObjectClass));
	}

	public Iterable<? extends DataObject> query(Filter... filters) {
		assert filters.length > 0 : "At least one filter is required";
		Criteria criteria = criteriaFromFilters(filters);

		LOGGER.debug("Firing query with critera {}", criteria.toString());
		return new DataObjectIterable(filters[0].getDataObjectClass(), criteria.list());
	}

	public Long count(Class<? extends DataObject> dataObjectClass) {
		return count(new Filter(dataObjectClass));
	}

	public Long count(Filter... filters) {
		assert filters.length > 0 : "At least one filter is required";
		Criteria criteria = criteriaFromFilters(filters);

		LOGGER.debug("Firing query with critera {}", criteria.toString());
		return (Long) criteria.setProjection(Projections.rowCount()).uniqueResult();
	}

	private Criteria criteriaFromFilters(Filter[] filters) {
		Criteria criteria = session.createCriteria(filters[0].getDataObjectClass());
		Arrays.asList(filters).forEach(filter -> {
			Criterion criterion = filter.getCriterion();
			if (criterion != null)
				criteria.add(criterion);
		});
		return criteria;
	}

	public void openTransaction() {
		this.transaction = session.beginTransaction();
	}

	public void massUpdate(DataObject dataObject) {
		assert session != null : "Session is null";
		assert dataObject != null : "No DataObject given";
		assert this.transaction != null : "No transaction";

		session.merge(dataObject);
	}

	public void commit() {
		if (this.transaction != null)
			this.transaction.commit();
	}

	public MetaData getMetaData(String identifer) {
		List<MetaData> list = session.createQuery("FROM MetaData metaData WHERE metaData.dataSourceIdentifier= :identifier")
				.setParameter("identifier", identifer).list();
		if (list.size() > 0) {
			return list.get(0);
		} else {
			return new MetaData(identifer);
		}
	}

	public void persistMetaData(MetaData metaData) {
		Transaction t = session.beginTransaction();
		session.saveOrUpdate(metaData);
		t.commit();
	}

}
