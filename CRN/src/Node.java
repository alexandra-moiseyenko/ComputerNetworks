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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

    private static final int TIMEOUT_MS = 5000;
    private static final int MAX_SENDS = 4;
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


        this.socket = new DatagramSocket(portNumber);
        this.portNumber = portNumber;
        this.nameToAddress.put(nodeName, ownAddress());
        this.localStore.put(nodeName, ownAddress());

    }

    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new IllegalStateException("port not opened");
        }

        if (delay < 0) {
            throw new IllegalArgumentException("Delay must be >= 0 ");
        }

        long endTime = delay > 0 ? System.currentTimeMillis() + delay : Long.MAX_VALUE;

        byte[] buffer = new byte[65535];

        while (true) {
            if (delay > 0) {
                long remaining = endTime - System.currentTimeMillis();
                if (remaining <= 0) return;
                socket.setSoTimeout((int) Math.min(remaining, Integer.MAX_VALUE));
            } else {
                socket.setSoTimeout(0);
            }

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
        String msg = new String(packet.getData(), 0 , packet.getLength(), StandardCharsets.ISO_8859_1);

        if (msg.length() < 4) return;
        if (msg.charAt(2) != ' ') return;

        String txid = msg.substring(0,2);

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
            case 'C':
                handleCAS(txid, rest.substring(1), packet);
                break;
            case 'V':
                handleRelay(txid, rest.substring(1), packet);
                break;
            case 'H':
                learnPassiveAddress(packet, parseNameResponse(msg));
                break;
            case 'O':
                learnAddressPairs(rest.substring(1));
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

        if (hasThreeStrictlyCloserNodes(name)) {
            sendTo(txid + " X X", req.getAddress(), req.getPort());
            return;
        }

        boolean replacing = localStore.containsKey(name);
        localStore.put(name, address);
        if (name.startsWith("N:")) {
            nameToAddress.put(name, address);
        }
        String response = txid + " X " + (replacing ? "R" : "A");
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleCAS(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 3);
        if (parts == null) return;
        String key = parts[0];
        String current = parts[1];
        String next = parts[2];
        String stored = localStore.get(key);
        String code;
        if (stored != null && stored.equals(current)) {
            localStore.put(key, next);
            code = "R";
        } else if (stored != null) {
            code = "N";
        } else if (isOneOfThreeClosest(key)) {
            localStore.put(key, next);
            if (key.startsWith("N:")) nameToAddress.put(key, next);
            code = "A";
        } else {
            code = "X";
        }
        String response = txid + " D " + code;
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleNearest(String txid, String fields, DatagramPacket req) throws Exception {
        String targetHashHex = fields.trim();
        if (targetHashHex.length() != 64) return;

        StringBuilder sb = new StringBuilder();
        sb.append(txid).append(" O ");

        byte[] targetHash = hexToBytes(targetHashHex);

        Map<String, String> candidates = new HashMap<>(nameToAddress);
        candidates.put(nodeName, ownAddress());
        List<Map.Entry<String, String>> closest = new ArrayList<>(candidates.entrySet());
        closest.sort(Comparator.comparingInt(e -> {
            try {
                return hashDistance(targetHash, HashID.computeHashID(e.getKey()));
            } catch (Exception ex) {
                return Integer.MAX_VALUE;
            }
        }));

        int limit = Math.min(3, closest.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, String> e = closest.get(i);
            sb.append(encodeCRNString(e.getKey())).append(encodeCRNString(e.getValue()));
        }

        sendTo(sb.toString(), req.getAddress(), req.getPort());
    }

    private void handleExists(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 1);
        if (parts == null) return;
        String key = parts[0];
        String result = localStore.containsKey(key) ? "Y" : "N";
        String response = txid + " F " + result;
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleRead(String txid, String fields, DatagramPacket req) throws Exception {
        String[] parts = parseCRNFields(fields, 1);
        if (parts == null) return;
        String key = parts[0];
        String value = localStore.get(key);
        String response = value != null
                ? txid + " S Y " + encodeCRNString(value)
                : txid + " S N";
        sendTo(response, req.getAddress(), req.getPort());
    }

    private void handleRelay(String txid, String fields, DatagramPacket req) throws Exception {
        String nextNode = parseFirstCRNField(fields);
        int firstLen = encodedFieldLength(fields);
        if (nextNode == null || firstLen < 0 || firstLen >= fields.length()) return;
        String innerMsg = fields.substring(firstLen);

        String addr = resolveAddress(nextNode);
        if (addr == null) {
            return;
        }

        new Thread(() -> {
            try (DatagramSocket relaySocket = new DatagramSocket()) {
                String innerTxid = innerMsg.length() >= 2 ? innerMsg.substring(0, 2) : "";
                char responseType = expectedResponseType(innerMsg);
                if (innerTxid.length() != 2 || responseType == 0) return;
                String response = requestResponseWithSocket(relaySocket, innerTxid, innerMsg, addr, responseType, TIMEOUT_MS);
                if (response != null && response.length() >= 2) {
                    response = txid + response.substring(2);
                    sendTo(response, req.getAddress(), req.getPort());
                }
            } catch (Exception ignored) {
            }
        }).start();
    }

    public synchronized boolean isActive(String nodeName) throws Exception {
        String addr = resolveAddress(nodeName);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " G ";
        String resp = requestResponse(txid, msg, addr, 'H');
        String responseName = parseNameResponse(resp == null ? "" : resp);
        return nodeName.equals(responseName);
    }

    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    public synchronized boolean exists(String key) throws Exception {
        if (localStore.containsKey(key)) return true;
        String txid = randomTxid();
        String msg = txid + " E " + encodeCRNString(key);
        String resp = null;
        for (String addr : knownAddresses()) {
            resp = requestResponse(txid, msg, addr, 'F');
            if (resp != null && resp.length() >= 6 && resp.charAt(5) == 'Y') break;
            txid = randomTxid();
            msg = txid + " E " + encodeCRNString(key);
        }
        if (resp == null || resp.length() < 6 || resp.charAt(5) != 'Y') {
            resp = requestResponse(txid, msg, findResponsibleNode(key), 'F');
        }
        return resp != null && resp.length() >= 6 && resp.charAt(5) == 'Y';
    }

    public synchronized String read(String key) throws Exception {
        if (localStore.containsKey(key)) return localStore.get(key);
        String txid = randomTxid();
        String msg = txid + " R " + encodeCRNString(key);
        String resp = null;
        for (String addr : knownAddresses()) {
            resp = requestResponse(txid, msg, addr, 'S');
            if (resp != null && resp.length() >= 6 && resp.charAt(5) == 'Y') break;
            txid = randomTxid();
            msg = txid + " R " + encodeCRNString(key);
        }
        if (resp == null || resp.length() < 6 || resp.charAt(5) != 'Y') {
            resp = requestResponse(txid, msg, findResponsibleNode(key), 'S');
        }
        if (resp != null && resp.length() >= 6 && resp.charAt(5) == 'Y') {
            String[] parts = parseCRNFields(resp.substring(7), 1);
            if (parts != null) return parts[0];
        }
        return null;
    }

    public synchronized boolean write(String key, String value) throws Exception {
        if (key.equals(nodeName)) {
            nameToAddress.put(key, value);
            localStore.put(key, value);
            return true;
        }

        String addr = findResponsibleNode(key);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " W " + encodeCRNString(key) + encodeCRNString(value);
        String resp = requestResponse(txid, msg, addr, 'X');
        if (resp == null || resp.length() < 6) return false;
        char code = resp.charAt(5);
        return code == 'R' || code == 'A';
    }

    public synchronized boolean CAS(String key, String currentValue, String newValue) throws Exception {
        String addr = findResponsibleNode(key);
        if (addr == null) return false;

        String txid = randomTxid();
        String msg = txid + " C " + encodeCRNString(key) + encodeCRNString(currentValue) + encodeCRNString(newValue);
        String resp = requestResponse(txid, msg, addr, 'D');
        if (resp == null || resp.length() < 6) return false;
        char code = resp.charAt(5);
        return code == 'R' || code == 'A';
    }

// Routing helpers

    /**
     * Find the address of the node responsible for storing a given key.
     * Uses a simple strategy: find the node in our table whose hash is closest
     * to the key's hash. Falls back to ourselves if the table is empty.
     */
    private String findResponsibleNode(String key) throws Exception {
        byte[] keyHash = HashID.computeHashID(key);
        discoverNearestNodes(keyHash);

        String bestName = nodeName;
        String bestAddr = ownAddress();
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

    private List<String> knownAddresses() {
        List<String> addresses = new ArrayList<>();
        for (Map.Entry<String, String> e : nameToAddress.entrySet()) {
            if (!e.getKey().equals(nodeName)) addresses.add(e.getValue());
        }
        return addresses;
    }
    /** Resolve a node name to its address. */
    private String resolveAddress(String name) throws Exception {
        if (name.equals(nodeName)) {
            return ownAddress();
        }
        return nameToAddress.get(name);
    }

    private String ownAddress() {
        String address = nameToAddress.get(nodeName);
        return address != null ? address : "127.0.0.1:" + portNumber;
    }

    // rejects storing keys when this node already knows three strictly closer nodes
    private boolean hasThreeStrictlyCloserNodes(String key) throws Exception {
        byte[] keyHash = HashID.computeHashID(key);
        int myDistance = hashDistance(keyHash, nodeHashId);
        int closer = 0;
        for (String name : nameToAddress.keySet()) {
            if (name.equals(nodeName)) continue;
            int d = hashDistance(keyHash, HashID.computeHashID(name));
            if (d < myDistance) closer++;
            if (closer >= 3) return true;
        }
        return false;
    }

    private boolean isOneOfThreeClosest(String key) throws Exception {
        return !hasThreeStrictlyCloserNodes(key);
    }

    // learns address pairs from name and nearest replies without an explicit write
    private void learnPassiveAddress(DatagramPacket packet, String responseNodeName) {
        if (responseNodeName != null && responseNodeName.startsWith("N:")) {
            nameToAddress.put(responseNodeName, packet.getAddress().getHostAddress() + ":" + packet.getPort());
        }
    }

    private String parseNameResponse(String resp) {
        if (resp.length() >= 6 && resp.charAt(3) == 'H') {
            String[] parts = parseCRNFields(resp.substring(5), 1);
            if (parts != null) return parts[0];
        }
        return null;
    }

    private String parseFirstCRNField(String fields) {
        String[] parts = parseCRNFields(fields, 1);
        return parts == null ? null : parts[0];
    }

    private int encodedFieldLength(String s) {
        int pos = 0;
        if (pos < s.length() && s.charAt(pos) == ' ') pos++;
        int start = pos;
        while (pos < s.length() && s.charAt(pos) != ' ') pos++;
        if (pos >= s.length()) return -1;
        int spaceCount;
        try {
            spaceCount = Integer.parseInt(s.substring(start, pos));
        } catch (NumberFormatException e) {
            return -1;
        }
        pos++;
        int spacesLeft = spaceCount;
        while (pos < s.length()) {
            char c = s.charAt(pos++);
            if (c == ' ') {
                if (spacesLeft > 0) {
                    spacesLeft--;
                } else {
                    return pos;
                }
            }
        }
        return -1;
    }

    // sends a request with rfc retry timing and filters replies by transaction id and type
    private String requestResponse(String txid, String msg, String addr, char responseType) throws Exception {
        InetAddress ia = parseAddress(addr);
        int port = parsePort(addr);
        for (int attempt = 0; attempt < MAX_SENDS; attempt++) {
            sendViaRelayOrDirect(msg, ia, port, findNameForAddress(addr));
            long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (System.currentTimeMillis() < deadline) {
                int remaining = (int)(deadline - System.currentTimeMillis());
                socket.setSoTimeout(remaining);
                byte[] buffer = new byte[65535];
                DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(pkt);
                    String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.ISO_8859_1);
                    if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == responseType) {
                        return resp;
                    }
                    handleSinglePacket(pkt);
                } catch (SocketTimeoutException e) {
                    break;
                }
            }
        }
        return null;
    }

    private char expectedResponseType(String msg) {
        if (msg.length() < 4 || msg.charAt(2) != ' ') return 0;
        switch (msg.charAt(3)) {
            case 'G': return 'H';
            case 'N': return 'O';
            case 'E': return 'F';
            case 'R': return 'S';
            case 'W': return 'X';
            case 'C': return 'D';
            case 'V':
                String fields = msg.substring(4);
                int len = encodedFieldLength(fields);
                if (len < 0 || len >= fields.length()) return 0;
                return expectedResponseType(fields.substring(len));
            default: return 0;
        }
    }

    private String findNameForAddress(String addr) {
        for (Map.Entry<String, String> e : nameToAddress.entrySet()) {
            if (e.getValue().equals(addr)) return e.getKey();
        }
        return null;
    }

    // relay forwarding uses its own socket so the main node can keep processing packets
    private String requestResponseWithSocket(DatagramSocket relaySocket, String txid, String msg, String addr, char responseType, int timeoutMs) throws Exception {
        byte[] data = msg.getBytes(StandardCharsets.ISO_8859_1);
        DatagramPacket out = new DatagramPacket(data, data.length, parseAddress(addr), parsePort(addr));
        relaySocket.send(out);
        relaySocket.setSoTimeout(timeoutMs);
        byte[] buffer = new byte[65535];
        DatagramPacket in = new DatagramPacket(buffer, buffer.length);
        try {
            relaySocket.receive(in);
            String resp = new String(in.getData(), 0, in.getLength(), StandardCharsets.ISO_8859_1);
            if (resp.length() >= 4 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == responseType) {
                return resp;
            }
        } catch (SocketTimeoutException e) {
            return null;
        }
        return null;
    }

    /**
     * rfc distance: 256 minus the number of matching leading bits
     * lower values are closer; equal hashes have distance 0
     */
    private int hashDistance(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int xor = (a[i] ^ b[i]) & 0xff;
            if (xor != 0) {
                return 256 - (i * 8 + Integer.numberOfLeadingZeros(xor) - 24);
            }
        }
        return 0;
    }

    private void discoverNearestNodes(byte[] keyHash) throws Exception {
        if (nameToAddress.size() <= 2) return;
        String targetHashHex = bytesToHex(keyHash);
        List<String> addresses = new ArrayList<>(nameToAddress.values());
        int queried = 0;
        for (String addr : addresses) {
            if (queried >= 6) break;
            queryNearest(addr, targetHashHex);
            queried++;
        }
    }

    // asks known peers for nearer address pairs before choosing where to send a key request
    private void queryNearest(String addr, String targetHashHex) throws Exception {
        String txid = randomTxid();
        String msg = txid + " N " + targetHashHex;
        sendTo(msg, parseAddress(addr), parsePort(addr));

        long deadline = System.currentTimeMillis() + 700;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int)(deadline - System.currentTimeMillis());
            if (remaining <= 0) return;
            socket.setSoTimeout(remaining);
            byte[] buffer = new byte[65535];
            DatagramPacket pkt = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(pkt);
                String resp = new String(pkt.getData(), 0, pkt.getLength(), StandardCharsets.ISO_8859_1);
                if (resp.length() >= 5 && resp.startsWith(txid) && resp.charAt(2) == ' ' && resp.charAt(3) == 'O') {
                    learnAddressPairs(resp.substring(5));
                    return;
                }
                handleSinglePacket(pkt);
            } catch (SocketTimeoutException e) {
                return;
            }
        }
    }

    // parses the repeated name/address fields returned by an o nearest response
    private void learnAddressPairs(String fields) {
        List<String> parsed = parseAllCRNFields(fields);
        for (int i = 0; i + 1 < parsed.size(); i += 2) {
            String name = parsed.get(i);
            String address = parsed.get(i + 1);
            if (name.startsWith("N:") && address.contains(":")) {
                nameToAddress.put(name, address);
            }
        }
    }
    /**
     * Send a message via the relay stack (if non-empty) or directly.
     * The relay stack: bottom = first relay, top = innermost relay.
     * We wrap the message from outermost relay outward.
     */
    private void sendViaRelayOrDirect(String msg, InetAddress dest, int destPort) throws Exception {
        sendViaRelayOrDirect(msg, dest, destPort, null);
    }

    private void sendViaRelayOrDirect(String msg, InetAddress dest, int destPort, String destName) throws Exception {
        if (relayStack.isEmpty()) {
            sendTo(msg, dest, destPort);
            return;
        }

        if (destName == null) {
            sendTo(msg, dest, destPort);
            return;
        }

        List<String> ordered = new ArrayList<>(relayStack);
        for (int left = 0, right = ordered.size() - 1; left < right; left++, right--) {
            String tmp = ordered.get(left);
            ordered.set(left, ordered.get(right));
            ordered.set(right, tmp);
        }
        String firstRelayAddr = nameToAddress.get(ordered.get(0));
        if (firstRelayAddr == null) {
            sendTo(msg, dest, destPort);
            return;
        }

        String innerPayload = msg;
        String nextName = destName;
        for (int i = ordered.size() - 1; i >= 0; i--) {
            String innerTxid = (i == 0 && msg.length() >= 2) ? msg.substring(0, 2) : randomTxid();
            innerPayload = innerTxid + " V " + encodeCRNString(nextName) + innerPayload;
            nextName = ordered.get(i);
        }

        InetAddress relayAddr = parseAddress(firstRelayAddr);
        int relayPort = parsePort(firstRelayAddr);
        sendTo(innerPayload, relayAddr, relayPort);
    }
    private void sendTo(String msg, InetAddress address, int port) throws Exception {
        byte[] data = msg.getBytes(StandardCharsets.ISO_8859_1);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
    }

    private String encodeCRNString(String s) {
        if (s == null) s = "";
        int spaces = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ' ') spaces++;
        }
        return spaces + " " + s + " ";
    }

    private String[] parseCRNFields(String s, int n) {
        String[] results = new String[n];
        int pos = 0;
        for (int i = 0; i < n; i++) {
            // skip leading space if present
            if (pos < s.length() && s.charAt(pos) == ' ') pos++;
            if (pos >= s.length()) return null;

            // read the space count number
            int numStart = pos;
            while (pos < s.length() && s.charAt(pos) != ' ') pos++;
            if (pos >= s.length()) return null;
            int spaceCount;
            try {
                spaceCount = Integer.parseInt(s.substring(numStart, pos));
            } catch (NumberFormatException e) {
                return null;
            }
            pos++; // skip the space after the number

            // the content is spaceCount spaces + spaceCount+1 non-space segments
            // total characters = length until we've consumed spaceCount spaces and end on ' '
            // read until we have consumed spaceCount internal spaces, then stop at next ' '
            int spacesLeft = spaceCount;
            StringBuilder sb = new StringBuilder();
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ') {
                    if (spacesLeft > 0) {
                        sb.append(' ');
                        spacesLeft--;
                        pos++;
                    } else {
                        pos++; // terminating space
                        break;
                    }
                } else {
                    sb.append(c);
                    pos++;
                }
            }
            results[i] = sb.toString();
        }
        return results;
    }

    // parses all crn string fields when the response can contain a variable number of fields.
    private List<String> parseAllCRNFields(String s) {
        List<String> results = new ArrayList<>();
        int pos = 0;
        while (pos < s.length()) {
            if (s.charAt(pos) == ' ') pos++;
            if (pos >= s.length()) break;

            int numStart = pos;
            while (pos < s.length() && s.charAt(pos) != ' ') pos++;
            if (pos >= s.length()) break;

            int spaceCount;
            try {
                spaceCount = Integer.parseInt(s.substring(numStart, pos));
            } catch (NumberFormatException e) {
                break;
            }
            pos++;

            int spacesLeft = spaceCount;
            StringBuilder sb = new StringBuilder();
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (c == ' ') {
                    if (spacesLeft > 0) {
                        sb.append(' ');
                        spacesLeft--;
                        pos++;
                    } else {
                        pos++;
                        break;
                    }
                } else {
                    sb.append(c);
                    pos++;
                }
            }
            results.add(sb.toString());
        }
        return results;
    }

    private String randomTxid() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        char c1 = chars.charAt(random.nextInt(chars.length()));
        char c2 = chars.charAt(random.nextInt(chars.length()));
        return "" + c1 + c2;
    }

    private InetAddress parseAddress(String addr) throws Exception {
        int colon = addr.lastIndexOf(':');
        return InetAddress.getByName(addr.substring(0, colon));
    }

    private int parsePort(String addr) {
        int colon = addr.lastIndexOf(':');
        return Integer.parseInt(addr.substring(colon + 1));
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

}