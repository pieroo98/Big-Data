package it.polito.bigdata.spark.example;

import java.io.Serializable;
import java.sql.Timestamp;

public class InfoStazione2 implements Serializable{

	private int station;
	private String GiornoEora;
	private int free_slots;
	
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
	public int getFree_slots() {
		return free_slots;
	}
	public void setFree_slots(int free_slots) {
		this.free_slots = free_slots;
	}
	
}
