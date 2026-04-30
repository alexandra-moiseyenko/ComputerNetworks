IN2011 Computer Networks Coursework
Name: Alexandra Moiseyenko
ID: 230065253
Email: alexandra.moiseyenko@city.ac.uk

Build Instructions
==================
1) Ensure all required files are in the same directory:
    Node.java
    HashID.java
    LocalTest.java
2) Open a terminal in this directory.
3) Compile the program using:
    javac *.java
There are no package declarations and no external library dependencies.
==========================================================
Run Instructions
================
The coursework marker should use the standard NodeInterface methods through Node.java.
4) To run the local test:
    java LocalTest
5) To run on the Azure lab environment:
    java AzureLabTest alexandra.moiseyenko@city.ac.uk <VM_IP>
    Example:
    java AzureLabTest alexandra.moiseyenko@city.ac.uk 10.216.34.196 20110
6) Notes:
    - Ensure the chosen port is within the allowed range (20110–20130).
    - The VM IP should be your assigned Azure Lab IP.
==========================================================================
Working Functionality
=====================
The implemented node provides a working CRN-25 distributed key-value storage system using UDP communication.

The following functionality is complete:
    Nodes can communicate using UDP sockets.
    Nodes maintain a mapping of node names to network addresses.
    Key-value storage is distributed across nodes.

The following operations are implemented and working:
    exists(key): correctly checks if a key exists in the network.
    read(key): retrieves the correct value for a given key.
    write(key, value): stores key-value pairs on the appropriate node.
    CAS(key, currentValue, newValue): updates values conditionally.

Additional functionality:
    Routing is implemented using SHA-256 hash distance.
    The closest node to a key is selected for storage and retrieval.
    Peer discovery works through nearest-node queries and address updates.
    The system supports relay messaging through a relay stack.
    CRN message encoding and decoding are correctly implemented.
    Timeouts and retries are used to handle unreliable UDP communication.

The implementation has been tested using LocalTest.java and successfully performs distributed read/write operations.
====================================================================================================================
Known Limitations
=================
- This is NOT a complete test of the coursework.
- In particular, it does not fully test:
  retransmission / timeout behaviour
  duplicate or reordered packets
  relay correctness
  malformed message robustness
  compare-and-swap edge cases
  all message types and all topologies
- Routing depends on known nodes. Incomplete network knowledge may reduce efficiency.
- Performance may vary depending on how many peers are discovered.
- UDP packet loss may still cause occasional failures despite retries.
- The relay mechanism is functional but not heavily optimised for large relay chains.
=====================================================================================
Testing
========
The implementation has been tested locally with the supplied Java tests.
    Also the Azure lab smoke test has been run on the Azure lab machine.
The test read all seven Jabberwocky poem entries from the lab network and successfully wrote and read back my marker value:
    D:alexandra.moiseyenko@city.ac.uk -> It works!
Wireshark evidence is included as capture.pcap.
    The capture was recorded on the Azure lab machine while the node was running.


