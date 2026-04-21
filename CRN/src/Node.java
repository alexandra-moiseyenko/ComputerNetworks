// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private byte[] nodeHashId;
    private DatagramSocket socket;
    private int portNumber;

    public void setNodeName(String nodeName) throws Exception {
        if(nodeName == null || !nodeName.startsWith("N:")){
            throw new IllegalArgumentException("Node name must start with N:");
        }
        this.nodeName = nodeName;
        this.nodeHashId = HashID.computeHashID(nodeName);

    }

    public void openPort(int portNumber) throws Exception {

        if(nodeName == null) {
          throw new IllegalStateException("SetNode first!");
        }

        if(portNumber < 20110 || portNumber > 20130) {
            throw new IllegalArgumentException("Enter port number within range of 20110 - 20130");
        }

        this.socket = new DatagramSocket(portNumber);
        this.portNumber = portNumber;

    }

    public void handleIncomingMessages(int delay) throws Exception {
	    if (socket == null) {
          throw new IllegalStateException("port not opened");
        }

        if (delay < 0) {
            throw new IllegalArgumentException("Delay must be >= 0 ");
        }

        socket.setSoTimeout(delay);

        byte[] buffer = new byte[65535];

        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try{
                socket.receive(packet);
                handleSinglePacket(packet);
            } catch (SocketException e) {
                return;
            } catch (Exception e) {

            }
        }
    }

    private void handleSinglePacket(DatagramPacket packet) throws Exception {
        String msg = new String(packet.getData(), 0 , packet.getLength(), StandardCharsets.UTF_8);

        if (msg.length() < 4) return;
        if (msg.charAt(2) != ' ') return;

        String txid = msg.substring(0,2);
        if (txid.charAt(0) == ' ' || txid.charAt(1) == ' ') return;

        String rest = msg.substring(3);
        if (rest.isEmpty()) return;

        char messageType = rest.charAt(0);

        switch (messageType) {
            case 'G':
                handleNameRequest(txid, packet);
                break;

            case 'w':
                // add write logic later
                break;

            default:
                break;
        }
    }

    private void handleNameRequest(String txid, DatagramPacket requestPacket) throws Exception {
        String response = txid + 'H' + encodeCRNString(nodeName);
        sendMessage(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    private void sendMessage(String msg, InetAddress address, int port) throws Exception {
        byte[] data = msg.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    private String encodeCRNString(String s) {
        if (s == null) {
            s = "";
        }
        int spaces = 0;
        for (int i = 0; i < s.length(); i ++) {
            if (s.charAt(i) == ' ') {
                spaces ++;
            }
        }
        return spaces + " " + s + " ";
    }


    
    public boolean isActive(String nodeName) throws Exception {
	handleIncomingMessages(1);
    String address =
    }
    
    public void pushRelay(String nodeName) throws Exception {
	throw new Exception("Not implemented");
    }

    public void popRelay() throws Exception {
        throw new Exception("Not implemented");
    }

    public boolean exists(String key) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public String read(String key) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean write(String key, String value) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
    }
}
