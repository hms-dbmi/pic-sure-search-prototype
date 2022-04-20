package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract model that will hold all required fields that need to be set for 
 * pic-sure-search to function.
 * 
 * All models need to set values for these fields.  They will be converted to the requried metadata
 * that needs to exist in the TopmedDataTable object that is stored in the dictionary.javabin
 * @author Tom
 *
 */
public abstract class DictionaryModel {

	// all fields are strings to help convert to the Map<String, String> in the topmed data table.
	public String derived_var_id = "";
	public String derived_var_name = "";
	public String derived_var_description = "";
	public String derived_group_id = "";
	public String derived_group_name = "";
	public String derived_group_description = "";
	public String derived_study_id = "";
	public String derived_study_description = "";
	public String columnmeta_data_type = "";
	public String is_stigmatized = "";
	public String columnmeta_min = "";
	public String columnmeta_max = "";
	public String columnmeta_observation_count = "";
	public String columnmeta_patient_count = "";
	public String columnmeta_hpds_path = "";
	public String hashed_var_id = "";
	
	protected List<String> values;
	public Map<String,String> metadata = new HashMap<String,String>();
	
	public DictionaryModel() {
		super();
	};
	
	
	public DictionaryModel(String derived_var_id, String derived_var_name, String derived_var_description,
			String derived_group_id, String derived_group_name, String derived_group_description,
			String derived_study_id, String derived_study_description, String columnmeta_data_type,
			String is_stigmatized, String columnmeta_min, String columnmeta_max, String columnmeta_observation_count,
			String columnmeta_patient_count, String columnmeta_hpds_path) {
		super();
		this.derived_var_id = derived_var_id;
		this.derived_var_name = derived_var_name;
		this.derived_var_description = derived_var_description;
		this.derived_group_id = derived_group_id;
		this.derived_group_name = derived_group_name;
		this.derived_group_description = derived_group_description;
		this.derived_study_id = derived_study_id;
		this.derived_study_description = derived_study_description;
		this.columnmeta_data_type = columnmeta_data_type;
		this.is_stigmatized = is_stigmatized;
		this.columnmeta_min = columnmeta_min;
		this.columnmeta_max = columnmeta_max;
		this.columnmeta_observation_count = columnmeta_observation_count;
		this.columnmeta_patient_count = columnmeta_patient_count;
		this.columnmeta_hpds_path = columnmeta_hpds_path;
	}
	/**
	 * Cloning method.  
	 * @param defaultJsonDictionaryModel
	 */
	public DictionaryModel(DefaultJsonDictionaryModel defaultJsonDictionaryModel) {
		this.derived_var_id = defaultJsonDictionaryModel.derived_var_id;
		this.derived_var_name = defaultJsonDictionaryModel.derived_var_name;
		this.derived_var_description = defaultJsonDictionaryModel.derived_var_description;
		this.derived_group_id = defaultJsonDictionaryModel.derived_group_id;
		this.derived_group_name = defaultJsonDictionaryModel.derived_group_name;
		this.derived_group_description = defaultJsonDictionaryModel.derived_group_description;
		this.derived_study_id = defaultJsonDictionaryModel.derived_study_id;
		this.derived_study_description = defaultJsonDictionaryModel.derived_study_description;
		this.columnmeta_data_type = defaultJsonDictionaryModel.columnmeta_data_type;
		this.is_stigmatized = defaultJsonDictionaryModel.is_stigmatized;
		this.columnmeta_min = defaultJsonDictionaryModel.columnmeta_min;
		this.columnmeta_max = defaultJsonDictionaryModel.columnmeta_max;
		this.columnmeta_observation_count = defaultJsonDictionaryModel.columnmeta_observation_count;
		this.columnmeta_patient_count = defaultJsonDictionaryModel.columnmeta_patient_count;
		this.columnmeta_hpds_path = defaultJsonDictionaryModel.columnmeta_hpds_path;	}


	public abstract Map<String, DictionaryModel> build();
	
	public abstract Map<String, DictionaryModel> build(String[] controlFileRow, Map<String,DictionaryModel> baseDictionary);

	public String getDerived_var_id() {
		return derived_var_id;
	}

	public void setDerived_var_id(String derived_var_id) {
		this.derived_var_id = derived_var_id;
	}

	public String getDerived_var_name() {
		return derived_var_name;
	}

	public void setDerived_var_name(String derived_var_name) {
		this.derived_var_name = derived_var_name;
	}

	public String getDerived_var_description() {
		return derived_var_description;
	}

	public void setDerived_var_description(String derived_var_description) {
		this.derived_var_description = derived_var_description;
	}

	public String getDerived_group_id() {
		return derived_group_id;
	}

	public void setDerived_group_id(String derived_group_id) {
		this.derived_group_id = derived_group_id;
	}

	public String getDerived_group_name() {
		return derived_group_name;
	}

	public void setDerived_group_name(String derived_group_name) {
		this.derived_group_name = derived_group_name;
	}

	public String getDerived_group_description() {
		return derived_group_description;
	}

	public void setDerived_group_description(String derived_group_description) {
		this.derived_group_description = derived_group_description;
	}

	public String getDerived_study_id() {
		return derived_study_id;
	}

	public void setDerived_study_id(String derived_study_id) {
		this.derived_study_id = derived_study_id;
	}

	public String getDerived_study_description() {
		return derived_study_description;
	}

	public void setDerived_study_description(String derived_study_description) {
		this.derived_study_description = derived_study_description;
	}

	public String getColumnmeta_data_type() {
		return columnmeta_data_type;
	}

	public void setColumnmeta_data_type(String columnmeta_data_type) {
		this.columnmeta_data_type = columnmeta_data_type;
	}

	public String getIs_stigmatized() {
		return is_stigmatized;
	}

	public void setIs_stigmatized(String is_stigmatized) {
		this.is_stigmatized = is_stigmatized;
	}

	public String getColumnmeta_min() {
		return columnmeta_min;
	}

	public void setColumnmeta_min(String columnmeta_min) {
		this.columnmeta_min = columnmeta_min;
	}

	public String getColumnmeta_max() {
		return columnmeta_max;
	}

	public void setColumnmeta_max(String columnmeta_max) {
		this.columnmeta_max = columnmeta_max;
	}

	public String getColumnmeta_observation_count() {
		return columnmeta_observation_count;
	}

	public void setColumnmeta_observation_count(String columnmeta_observation_count) {
		this.columnmeta_observation_count = columnmeta_observation_count;
	}

	public String getColumnmeta_patient_count() {
		return columnmeta_patient_count;
	}

	public void setColumnmeta_patient_count(String columnmeta_patient_count) {
		this.columnmeta_patient_count = columnmeta_patient_count;
	}


	/** 
	 * Columnmeta is the base dictionary generated which this method is meant to do
	 * could be used if another base dictionary is used instead.
	 */
	public Map<String, DictionaryModel> build(Map<String, DictionaryModel> baseDictionary) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
}
