package edu.harvard.hms.dbmi.avillach.hpds.etl.dict.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract model that will hold all required fields that need to be set for 
 * pic-sure-search to function.
 * 
 * All models need to set values for these fields.  They will be converted to the required metadata
 * that needs to exist in the TopmedDataTable object and it's children objects that is stored in the dictionary.javabin
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
	public String derived_study_abv_name = "";
	public String columnmeta_data_type = "";
	public String columnmeta_is_stigmatized = "";
	public String columnmeta_min = "";
	public String columnmeta_max = "";
	public String columnmeta_observation_count = "";
	public String columnmeta_patient_count = "";
	public String columnmeta_hpds_path = "";
	public String hashed_var_id = "";
	public String data_hierarchy = "";
	//public Map<String, String> valuesMap;
	public List<String> values;
	// removing this for now as dynamic metadata will be built out after release
	public Map<String,String> derived_variable_level_data = new HashMap<String,String>();
	
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
		this.columnmeta_is_stigmatized = is_stigmatized;
		this.columnmeta_min = columnmeta_min;
		this.columnmeta_max = columnmeta_max;
		this.columnmeta_observation_count = columnmeta_observation_count;
		this.columnmeta_patient_count = columnmeta_patient_count;
		this.columnmeta_hpds_path = columnmeta_hpds_path;
	}
	/**
	 * Cloning method.  
	 * 
	 * The Internal Json Dictionaries uses this to populate it's required fields.  This is a bit more straight forward
	 * then other models.  Could potentially switch other models to use the same methodology.
	 * 
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
		this.columnmeta_is_stigmatized = defaultJsonDictionaryModel.columnmeta_is_stigmatized;
		this.columnmeta_min = defaultJsonDictionaryModel.columnmeta_min;
		this.columnmeta_max = defaultJsonDictionaryModel.columnmeta_max;
		this.columnmeta_observation_count = defaultJsonDictionaryModel.columnmeta_observation_count;
		this.columnmeta_patient_count = defaultJsonDictionaryModel.columnmeta_patient_count;
		this.columnmeta_hpds_path = defaultJsonDictionaryModel.columnmeta_hpds_path;	
	}
	
	public DictionaryModel(DefaultJsonDictionaryModel_NewFormat defaultJsonDictionaryModel) {
		this.derived_var_id = defaultJsonDictionaryModel.derived_var_id;
		this.derived_var_name = defaultJsonDictionaryModel.derived_var_name;
		this.derived_var_description = defaultJsonDictionaryModel.derived_var_description;
		this.derived_group_id = defaultJsonDictionaryModel.derived_group_id;
		this.derived_group_name = defaultJsonDictionaryModel.derived_group_name;
		this.derived_group_description = defaultJsonDictionaryModel.derived_group_description;
		this.derived_study_id = defaultJsonDictionaryModel.derived_study_id;
		this.derived_study_description = defaultJsonDictionaryModel.derived_study_description;
		this.columnmeta_data_type = defaultJsonDictionaryModel.columnmeta_data_type;
		this.columnmeta_is_stigmatized = defaultJsonDictionaryModel.columnmeta_is_stigmatized;
		this.columnmeta_min = defaultJsonDictionaryModel.columnmeta_min;
		this.columnmeta_max = defaultJsonDictionaryModel.columnmeta_max;
		this.columnmeta_observation_count = defaultJsonDictionaryModel.columnmeta_observation_count;
		this.columnmeta_patient_count = defaultJsonDictionaryModel.columnmeta_patient_count;
		this.columnmeta_hpds_path = defaultJsonDictionaryModel.columnmeta_hpds_path;
		this.data_hierarchy = defaultJsonDictionaryModel.data_hierarchy;
		this.derived_variable_level_data = defaultJsonDictionaryModel.derived_variable_level_data;
	}
	
	/**
	 * Cloning method.
	 * 
	 * The Internal Json Dictionaries uses this to populate it's required fields.
	 * This is a bit more straight forward
	 * then other models. Could potentially switch other models to use the same
	 * methodology.
	 * 
	 * @param datatableJsonDictionaryModel
	 */
	public DictionaryModel(DatatableJsonDictionaryModel datatableJsonDictionaryModel) {
		this.derived_var_id = datatableJsonDictionaryModel.derived_var_id;
		this.derived_var_name = datatableJsonDictionaryModel.derived_var_name;
		this.derived_var_description = datatableJsonDictionaryModel.derived_var_description;
		this.derived_group_id = datatableJsonDictionaryModel.derived_group_id;
		this.derived_group_name = datatableJsonDictionaryModel.derived_group_name;
		this.derived_group_description = datatableJsonDictionaryModel.derived_group_description;
		this.derived_study_id = datatableJsonDictionaryModel.derived_study_id;
		this.derived_study_description = datatableJsonDictionaryModel.derived_study_description;
		this.columnmeta_data_type = datatableJsonDictionaryModel.columnmeta_data_type;
		this.columnmeta_is_stigmatized = datatableJsonDictionaryModel.columnmeta_is_stigmatized;
		this.columnmeta_min = datatableJsonDictionaryModel.columnmeta_min;
		this.columnmeta_max = datatableJsonDictionaryModel.columnmeta_max;
		this.columnmeta_observation_count = datatableJsonDictionaryModel.columnmeta_observation_count;
		this.columnmeta_patient_count = datatableJsonDictionaryModel.columnmeta_patient_count;
		this.columnmeta_hpds_path = datatableJsonDictionaryModel.columnmeta_hpds_path;
		this.data_hierarchy = datatableJsonDictionaryModel.data_hierarchy;
		this.derived_variable_level_data = datatableJsonDictionaryModel.derived_variable_level_data;
	}

	/**
	 * base method to build dictionary models not used 
	 * @return
	 */
	public abstract Map<String, DictionaryModel> build();
	
	/** 
	 * The base dictionary is generated via the columnmeta data stored in HPDS resource.
	 * The dictionary resource needs to align with the HPDS resource or else the application will break
	 *
	 * Only the ColumnMetaDictionaryModel should be calling this method at this time unless another source of 
	 * truth for what is stored in HPDS besides the columnMeta data is used.  
	 */
	public abstract Map<String, DictionaryModel> build(Map<String, DictionaryModel> baseDictionary);


	/**
	 * 
	 * This method needs to be used by each Model as it builds the dictionary model and updates
	 * the base dictionaries generated  
	 * 
	 * @param controlFileRow
	 * @param baseDictionary
	 * @return
	 */
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
		return columnmeta_is_stigmatized;
	}

	public void setIs_stigmatized(String is_stigmatized) {
		this.columnmeta_is_stigmatized = is_stigmatized;
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

	public List<String> getValues() {
		return values;
	}


	public void setValues(List<String> values) {
		this.values = values;
	}
	
	
	
}
