package edu.harvard.hms.dbmi.avillach.hpds.model.domain;

import java.util.Map;
import java.util.UUID;

public class QueryStatus {
	private PicSureStatus status;

	/**
	 * a uuid associated to a Resource in the database
	 */
	private UUID resourceID;
	
	private String resourceStatus;

	/**
	 * when user makes a query, a corresponding Result uuid is generated
	 */
	private UUID picsureResultId;

	/**
	 * when a resource might generate its own resultId and return it,
	 * we can keep it here
	 */
	private String resourceResultId;

	/**
	 * any metadata will be stored here
	 */
	private Map<String, Object> resultMetadata;

	private long sizeInBytes;
	
	private long startTime;
	
	private long duration;
	
	private long expiration;

	public PicSureStatus getStatus() {
		return status;
	}

	public void setStatus(PicSureStatus status) {
		this.status = status;
	}

	public UUID getResourceID() {
		return resourceID;
	}

	public void setResourceID(UUID resourceID) {
		this.resourceID = resourceID;
	}

	public String getResourceStatus() {
		return resourceStatus;
	}

	public void setResourceStatus(String resourceStatus) {
		this.resourceStatus = resourceStatus;
	}

	public long getSizeInBytes() {
		return sizeInBytes;
	}

	public void setSizeInBytes(long sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public long getExpiration() {
		return expiration;
	}

	public void setExpiration(long expiration) {
		this.expiration = expiration;
	}

	public UUID getPicsureResultId() {
		return picsureResultId;
	}

	public void setPicsureResultId(UUID picsureResultId) {
		this.picsureResultId = picsureResultId;
	}

	public String getResourceResultId() {
		return resourceResultId;
	}

	public void setResourceResultId(String resourceResultId) {
		this.resourceResultId = resourceResultId;
	}

	public Map<String, Object> getResultMetadata() {
		return resultMetadata;
	}

	public void setResultMetadata(Map<String, Object> resultMetadata) {
		this.resultMetadata = resultMetadata;
	}
}
