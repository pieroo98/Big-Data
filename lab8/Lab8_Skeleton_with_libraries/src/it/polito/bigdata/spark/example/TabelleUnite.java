package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class TabelleUnite implements Serializable{

	private int station;
	private double longitude;
	private double latitude;
	private String GiornoEora;
	private double criticita;
	
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
	public String getGiornoEora() {
		return GiornoEora;
	}
	public void setGiornoEora(String giornoEora) {
		GiornoEora = giornoEora;
	}
	public double getCriticita() {
		return criticita;
	}
	public void setCriticita(double criticita) {
		this.criticita = criticita;
	}

}
