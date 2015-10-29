package com.github.heussd.lodicity.store;

import java.util.ArrayList;

import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Restrictions;

import com.github.heussd.lodicity.model.DataObject;
import com.github.heussd.lodicity.model.Schema;

/**
 * TODO: introduce schema-verification
 * @author th
 *
 */
public class Filter {

	private Class<? extends DataObject> dataObjectClass;
	private ArrayList<Criterion> criterions = new ArrayList<>();
	private Criterion criterion;

	public Filter(Class<? extends DataObject> dataObjectClass) {
		this.dataObjectClass = dataObjectClass;
	}

	private Filter(Class<? extends DataObject> dataObjectClass, Criterion criterion) {
		this.dataObjectClass = dataObjectClass;
		this.criterion = criterion;
	}

	public Criterion getCriterion() {
		return this.criterion;
	}

	public Filter eq(String field, Object value) {
		if (Schema.isListType(dataObjectClass, field)) {
			return new Filter(dataObjectClass, Restrictions.like("subject", "%\"" + value + "\"%"));
		} else {
			return new Filter(dataObjectClass, Restrictions.eq(field, value));
		}
	}

	public Filter ilike(String field, String iLikeExpression) {
		return new Filter(dataObjectClass, Restrictions.ilike(field, "%" + iLikeExpression + "%"));
	}

	public Filter or(Filter... filters) {
		Criterion[] cs = new Criterion[filters.length];
		for (int i = 0; i < filters.length; i++) {
			criterions.remove(filters[i]);
			cs[i] = filters[i].getCriterion();
		}
		return new Filter(dataObjectClass, Restrictions.or(cs));
	}

	public Class<? extends DataObject> getDataObjectClass() {
		return this.dataObjectClass;
	}

	public Filter like(String string, String string2) {
		// TODO Auto-generated method stub
		return null;
	}

}
