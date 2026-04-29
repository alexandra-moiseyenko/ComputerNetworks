// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Alexandra Moiseyenko
//  230065253
//  alexandra.moiseyenko@city.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.net.*;
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

    private final Map<String, String> nameToAddress = new HashMap<>();
    private final Map<String, String> localStore = new HashMap<>();
    private final Deque<String> relayStack = new ArrayDeque<>();

    private static final int TIMEOUT_MS = 2000;
    private final Random random = new Random();

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
            } catch (SocketTimeoutException e) {
                return;
            } catch (SocketException e) {
                return;
            } catch (Exception e) {
                // Malformed packet – ignore and keep listening
            }
        }
    }

    private boolean receiveOne(int timeoutMs) throws Exception {
        socket.setSoTimeout(timeoutMs);
        byte[] buffer = new byte[65535];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        try {
            socket.receive(packet);
            handleSinglePacket(packet);
            return true;
        } catch (SocketTimeoutException e) {
            return false;
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
                handleGetName(txid, packet);
                break;
            case 'W':
                handleWriteAddress(txid, rest.substring(1), packet);
                break;
            case 'N':
                handleNearest(txid, rest.substring(1), packet);
                break;
            case 'E':
                handleExists(txid, rest.substring(1), packet);
                break;
            case 'R':
                handleRead(txid, rest.substring(1), packet);
                break;
            case 'w':
                handleWriteData(txid, rest.substring(1), packet);
                break;
            case 'c':
                handleCAS(txid, rest.substring(1), packet);
                break;
            case 'V':
                handleRelay(txid, rest.substring(1), packet);
                break;
            // lowercase responses are handled in the send/receive loops
            default:
                break;
        }
    }
    private void handleGetName(String txid, DatagramPacket req) throws Exception {
        String response = txid + " H " + encodeCRNString(nodeName);
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleWriteAddress(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 2);
        if (parts == null) return;
        String name = parts[0];
        String address = parts[1];
        nameToAddress.put(name, address);
        String response = txid + " w " + encodeCRNString(name);
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleNearest(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 1);
        if (parts == null) return;
        String targetHashHex = parts[0];

        StringBuilder sb = new StringBuilder();
        sb.append(txid).append(" n ");

        String myAddr = "127.0.0.1:" + portNumber;
        sb.append(encodeCRNString(nodeName)).append(encodeCRNString(myAddr));

        int added = 0;
        byte[] targetHash = hexToBytes(targetHashHex);
        String closestName = null;
        String closestAddr = null;
        int closestDist = Integer.MAX_VALUE;
        for (Map.Entry<String, String> e : nameToAddress.entrySet()) {
            byte[] h = HashID.computeHashID(e.getKey());
            int d = hashDistance(targetHash, h);
            if (d < closestDist) {
                closestDist = d;
                closestName = e.getKey();
                closestAddr = e.getValue();
            }
        }
        if (closestName != null) {
            sb.append(encodeCRNString(closestName)).append(encodeCRNString(closestAddr));
        }

        sendTo(sb.toString(), req.getAddress(), req.getPort());
    }

    private void handleExists(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 1);
        if (parts == null) return;
        String key = parts[0];
        String result = localStore.containsKey(key) ? "Y" : "N";
        String response = txid + " e " + encodeCRNString(result);
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleRead(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 1);
        if (parts == null) return;
        String key = parts[0];
        String value = localStore.get(key);
        String response = txid + " r " + encodeCRNString(value != null ? value : "");
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleWriteData(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 2);
        if (parts == null) return;
        String key = parts[0];
        String value = parts[1];
        localStore.put(key, value);
        String response = txid + " W " + encodeCRNString(key);
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleCAS(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 3);
        if (parts == null) return;
        String key = parts[0];
        String current = parts[1];
        String next = parts[2];
        boolean success = false;
        String stored = localStore.get(key);
        if (stored != null && stored.equals(current)) {
            localStore.put(key, next);
            success = true;
        }
        String response = txid + " C " + encodeCRNString(success ? "Y" : "N");
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleRelay(String txid, String fields, DatagramPacket req) throws Exception {
        // Fields: <next-node-name> <message>
        String[] parts = parseCRNFields(fields, 2);
        if (parts == null) return;
        String nextNode = parts[0];
        String innerMsg = parts[1];

        String addr = nameToAddress.get(nextNode);
        if (addr == null) {
            // Can't relay – ack with failure
            String response = txid + " v " + encodeCRNString("N");
            sendTo(response, req.getAddress(), req.getPort());
            return;
        }
        String[] hp = addr.split(":");
        InetAddress ia = InetAddress.getByName(hp[0]);
        int port = Integer.parseInt(hp[1]);
        sendTo(innerMsg, ia, port);
        String response = txid + " v " + encodeCRNString("Y");
        sendTo(response, req.getAddress(), req.getPort());
    }

    public boolean isActive(String nodeName) throws Exception {
        String addr = resolveAddress(nodeName);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " G ";
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);

        sendViaRelayOrDirect(msg, ia, port);

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) break;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'H') {
                    return true;
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return false;
    }
    
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    public boolean exists(String key) throws Exception {
        String addr = findResponsibleNode(key);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " E " + encodeCRNString(key);
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);

        sendViaRelayOrDirect(msg, ia, port);

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) break;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'e') {
                    String[] parts = parseCRNFields(resp.substring(5), 1);
                    if (parts != null) return "Y".equals(parts[0]);
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return false;
    }
    
    public String read(String key) throws Exception {
        String addr = findResponsibleNode(key);
        if (addr == null) return null;

        String txid = randomTxid();
        String msg = txid + " R " + encodeCRNString(key);
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);

        sendViaRelayOrDirect(msg, ia, port);

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) break;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'r') {
                    String[] parts = parseCRNFields(resp.substring(5), 1);
                    if (parts != null) return parts[0].isEmpty() ? null : parts[0];
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return null;
    }

    public boolean write(String key, String value) throws Exception {
        String addr = findResponsibleNode(key);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " w " + encodeCRNString(key) + encodeCRNString(value);
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);

        sendViaRelayOrDirect(msg, ia, port);

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) break;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'W') {
                    return true;
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return false;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        String addr = findResponsibleNode(key);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " c " + encodeCRNString(key) + encodeCRNString(currentValue) + encodeCRNString(newValue);
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);

        sendViaRelayOrDirect(msg, ia, port);

        long deadline = System.currentTimeMillis() + TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) break;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.UTF_8);
                if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'C') {
                    String[] parts = parseCRNFields(resp.substring(5), 1);
                    if (parts != null) return "Y".equals(parts[0]);
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                break;
            }
        }
        return false;
    }

// Routing helpers

    /**
     * Find the address of the node responsible for storing a given key.
     * Uses a simple strategy: find the node in our table whose hash is closest
     * to the key's hash. Falls back to ourselves if the table is empty.
     */
    private String findResponsibleNode(String key) throws Exception {
        byte[] keyHash = HashID.computeHashID(key);
        String bestName = nodeName;
        String bestAddr = "127.0.0.1:" + portNumber;
        int bestDist = hashDistance(keyHash, nodeHashId);

        for (Map.Entry<String, String> e : nameToAddress.entrySet()) {
            byte[] h = HashID.computeHashID(e.getKey());
            int d = hashDistance(keyHash, h);
            if (d < bestDist) {
                bestDist = d;
                bestName = e.getKey();
                bestAddr = e.getValue();
            }
        }
        return bestAddr;
    }
    /** Resolve a node name to its address. */
    private String resolveAddress(String name) throws Exception {
        if (name.equals(nodeName)) {
            return "127.0.0.1:" + portNumber;
        }
        return nameToAddress.get(name);
    }

    /**
     * XOR distance metric: returns the index of the first differing byte
     * (higher = closer). Returns 0 for identical.
     */
    private int hashDistance(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            if (a[i] != b[i]) {
                // Use XOR of first differing byte as tie-breaker
                return i;
            }
        }
        return len;
    }
    /**
     * Send a message via the relay stack (if non-empty) or directly.
     * The relay stack: bottom = first relay, top = innermost relay.
     * We wrap the message from outermost relay outward.
     */
    private void sendViaRelayOrDirect(String msg, InetAddress dest, int destPort) throws Exception {
        if (relayStack.isEmpty()) {
            sendTo(msg, dest, destPort);
            return;
        }

        // Build relay chain: wrap message in V relay envelopes
        // relayStack.peek() is top (last pushed = first to contact)
        // We need to relay through the stack from bottom to top
        // Actually, stack: push A then push B means B is on top.
        // The relay chain: this node → A → B → dest
        // So we wrap: to A with payload "to B with payload 'msg to dest'"
        // Build as an array from bottom to top
        String[] relays = relayStack.toArray(new String[0]);
        // toArray on ArrayDeque gives top-first order (LIFO), so reverse for bottom-first
        // Actually Deque.toArray() is unspecified - let's iterate properly
        String[] ordered = new String[relays.length];
        int idx = 0;
        for (String s : relayStack) {
            ordered[idx++] = s; // ArrayDeque iterator goes from head (top of stack)
        }
        // ordered[0] = top of stack (last pushed, contacted first)
        // ordered[last] = bottom of stack (first pushed, contacted last before dest)
        // Chain: this -> ordered[0] -> ordered[1] -> ... -> ordered[last] -> dest

        // Innermost envelope: message to dest
        // We don't relay the final hop via V; dest receives the real message
        // So: wrap msg for dest inside relay chain from innermost outward
        // ordered[last] sends to dest, so wrap: V <dest-name> <msg>
        // Then wrap that for ordered[last-1], etc.
        // But we need the dest node name... we only have the address.
        // For simplicity, send directly since we have the address.
        // Relay is: ordered[0] relays to ordered[1] ... to dest
        // We send to ordered[0]'s address with V-wrapped message

        // Find address of first relay (ordered[0])
        String firstRelayAddr = nameToAddress.get(ordered[0]);
        if (firstRelayAddr == null) {
            // Can't relay, send direct
            sendTo(msg, dest, destPort);
            return;
        }

        // Build the fully wrapped message
        String wrapped = msg;
        // Wrap from the destination back to the first relay
        // Destination address string
        String destAddrStr = dest.getHostAddress() + ":" + destPort;
        // We'll encode the target address as the node identifier for relay
        // Wrap: for each relay from innermost (last) to outermost (first)
        // Inner relay sends to dest directly
        // For now: single relay level - send V <nextAddr> <wrappedMsg> to first relay
        // Multi-hop: chain V envelopes
        // Final hop: the last relay node will send 'msg' to dest
        String innerPayload = msg;
        // Wrap from ordered[last] toward ordered[0]
        for (int i = ordered.length - 1; i >= 0; i--) {
            String nextAddr;
            if (i == ordered.length - 1) {
                nextAddr = destAddrStr;
            } else {
                nextAddr = nameToAddress.get(ordered[i + 1]);
                if (nextAddr == null) nextAddr = destAddrStr;
            }
            String innerTxid = randomTxid();
            innerPayload = innerTxid + " V " + encodeCRNString(ordered[i]) + encodeCRNString(innerPayload);
        }

        InetAddress relayAddr = parseAddress(firstRelayAddr);
        int relayPort = parsePort(firstRelayAddr);
        sendTo(innerPayload, relayAddr, relayPort);
    }

}
