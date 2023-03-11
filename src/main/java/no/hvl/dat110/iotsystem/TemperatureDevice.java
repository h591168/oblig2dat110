package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.TODO;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor sn = new TemperatureSensor();

		System.out.println("Tempature device starting... ");
		
		Client client = new Client("sensor", Common.BROKERHOST, Common.BROKERPORT);
		client.connect();
		
		for(int i = 0; i < COUNT; i++) {
			int temp = sn.read();
			Logger.log("READING: " + temp);
			client.publish("temperature", ""+ temp);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
			client.disconnect();
			
		System.out.println("Temperature device stopping ... ");

		

	}
}
