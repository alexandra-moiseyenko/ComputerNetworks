Build Instructions
==================
1) Ensure all required files are in the same directory:
    Node.java
    HashID.java
    LocalTest.java
2) Open a terminal in this directory.
3) Compile the program using:
    javac *.java
4) To run the local test:
    java LocalTest
5) To run on the Azure lab environment:
    java AzureLabTest alexandra.moiseyenko@city.ac.uk <VM_IP>
    Example:
    java AzureLabTest alexandra.moiseyenko@city.ac.uk 10.216.34.196 20110
6) Ensure the chosen port is within the allowed range (20110–20130).
==========================================================================
Working Functionality
=====================
The implemented node provides a working CRN-25 distributed key-value storage system using UDP communication.

The following functionality is supported:

- Nodes can communicate with each other using UDP sockets.
- Nodes maintain a mapping of known node names to their network addresses.
- Nodes can store and retrieve key-value pairs across the network.
- The following operations are implemented and function correctly:
    exists(key): checks whether a key exists in the network.
    read(key): retrieves the value associated with a key.
    write(key, value): stores a key-value pair in the appropriate node.
    CAS(key, currentValue, newValue): updates a value only if it matches the expected current value.
- Routing is based on hash distance using SHA-256:
    Each key and node name is hashed.
    The node with the smallest hash distance to the key is selected.
- Peer discovery is supported:
    Nodes learn about other nodes through write-address (W) messages and nearest-node (N/O) queries.
    The network knowledge improves over time.
- The system handles unreliable UDP communication by:
    Using transaction IDs to match requests and responses.
    Implementing timeouts and multiple retries.
- Relay functionality is supported:
    Messages can be forwarded through intermediate nodes using a relay stack.
- CRN message encoding is correctly implemented:
    Strings are encoded using a space-count format.
    Messages are parsed reliably using custom parsing functions.
- The implementation has been tested using the provided LocalTest.java and successfully performs distributed read and write operations across multiple nodes.

Overall, the node correctly implements the core CRN-25 protocol features and demonstrates reliable distributed storage and communication.

