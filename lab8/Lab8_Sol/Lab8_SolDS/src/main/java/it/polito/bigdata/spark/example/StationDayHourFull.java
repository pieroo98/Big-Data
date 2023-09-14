package it.polito.bigdata.spark.example;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StationDayHourFull implements Serializable {
	private int station;
	private String dayofweek;
	private int hour;
	private int fullstatus;

	public int getStation() {
		return station;
	}

	public void setStation(int station) {
		this.station = station;
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

	public int getFullstatus() {
		return fullstatus;
	}

	public void setFullstatus(int fullstatus) {
		this.fullstatus = fullstatus;
	}

}
