package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FinalRecord implements Serializable {
	private int station;
	private String dayofweek;
	private int hour;
	private double longitude;
	private double latitude;
	private double criticality;

	public int getStation() {
		return station;
	}

	public void setStation(int station) {
		this.station = station;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getCriticality() {
		return criticality;
	}

	public void setCriticality(double criticality) {
		this.criticality = criticality;
	}

	public String getDayofweek() {
		return dayofweek;
	}

	public void setDayofweek(String dayofweek) {
		this.dayofweek = dayofweek;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

}
