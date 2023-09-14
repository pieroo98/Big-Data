package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class InfoStazione3 implements Serializable{

	private int station;
	private String GiornoEora;
	private double criticita;
	
	public int getStation() {
		return station;
	}
	public void setStation(int station) {
		this.station = station;
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
