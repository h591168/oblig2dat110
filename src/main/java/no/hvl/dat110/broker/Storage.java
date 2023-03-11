package no.hvl.dat110.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.common.TODO;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.messagetransport.Connection;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object
	
	protected ConcurrentHashMap<String, ClientSession> clients;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	// get the session object for a given user
	// session object can be used to send a message to the user
	
	public ClientSession getSession(String user) {

		ClientSession session = clients.get(user);

		return session;
	}


	public void addClientSession(String user, Connection connection) {
		ClientSession session = new ClientSession(user, connection);
		clients.put(user, session);
		
//		throw new UnsupportedOperationException(TODO.method());
		
	}

	public void removeClientSession(String user) {
		ClientSession session = clients.remove(user);
		if (session != null) {
			session.disconnect();
		}
		
//		throw new UnsupportedOperationException(TODO.method());
		
	}

	public void createTopic(String topic) {
		subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet());
		

//		throw new UnsupportedOperationException(TODO.method());
	
	}

	public void deleteTopic(String topic) {
		subscriptions.remove(topic);

//		throw new UnsupportedOperationException(TODO.method());
		
	}

	public void addSubscriber(String user, String topic) {
		Set<String> subscribers = subscriptions.computeIfAbsent(topic, k  -> ConcurrentHashMap.newKeySet());
		subscribers.add(user);
		
//		throw new UnsupportedOperationException(TODO.method());
		
	}

	public void removeSubscriber(String user, String topic) {
		Set<String> subscribers = subscriptions.get(topic);
		if (subscribers != null) {
			subscribers.remove(user);
		}
		
//		throw new UnsupportedOperationException(TODO.method());
	}
	public Set<String> getSubscribers(String topic) {
		Set<String> subscribers = subscriptions.get(topic);
		return (subscribers != null) ? subscribers : Collections.emptySet();
	}
	
}
