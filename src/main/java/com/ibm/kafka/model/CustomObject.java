package com.ibm.kafka.model;

import java.beans.Transient;
import java.io.Serializable;

public class CustomObject implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String id;
	private String invoiceNumber;
	private String stockCode;
	private String description;
	private Integer quantity;
	private String invoiceDate;
	private Double unitPrice;
	private String customeId;
	private String countryName;

	public CustomObject() {
		super();
	}

	public CustomObject(String[] values) {
		this.invoiceNumber = values[0];
		this.stockCode = values[1];
		this.description = values[2];
		this.quantity = Integer.getInteger(values[3]);
		this.invoiceDate = values[4];
		this.unitPrice = Double.valueOf(values[5]);
		this.customeId = values[6];
		this.countryName = values[7];
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getInvoiceNumber() {
		return invoiceNumber;
	}

	public void setInvoiceNumber(String invoiceNumber) {
		this.invoiceNumber = invoiceNumber;
	}

	public String getStockCode() {
		return stockCode;
	}

	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}

	public String getInvoiceDate() {
		return invoiceDate;
	}

	public void setInvoiceDate(String invoiceDate) {
		this.invoiceDate = invoiceDate;
	}

	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}

	public String getCustomeId() {
		return customeId;
	}

	public void setCustomeId(String customeId) {
		this.customeId = customeId;
	}

	public String getCountryName() {
		return countryName;
	}

	public void setCountryName(String countryName) {
		this.countryName = countryName;
	}
	@Transient
	public String getCountrySpecificTopic() {
		if (getCountryName() == null || getCountryName().isEmpty()) {
			return "DEFAULT_TOPIC";
		} else {
			return getCountryName().replaceAll(" ", "_").toUpperCase();
		}

	}

	@Override
	public String toString() {
		return "CustomObject [id=" + id + ", invoiceNumber=" + invoiceNumber + ", stockCode=" + stockCode
				+ ", description=" + description + ", quantity=" + quantity + ", invoiceDate=" + invoiceDate
				+ ", unitPrice=" + unitPrice + ", customeId=" + customeId + ", countryName=" + countryName + "]";
	}

}
