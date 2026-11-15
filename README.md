# Reliable Transport Protocol over UDP

## 📌 Overview
This project implements a custom **connection-oriented, reliable transport protocol** built on top of the unreliable UDP (User Datagram Protocol). The protocol provides an error-free, sequential byte-stream transfer between a Client and a Server, specifically designed for transmitting files (images, books) across a simulated lossy network.

Unlike standard assignments where the protocol specification is provided, this project required **designing the protocol specification from scratch**, focusing on packet structure, flow control, and error recovery.

## 🚀 Features & Capabilities

* **Connection-Oriented Data Transfer:** Establishes a logical connection before data transmission begins, ensuring both ends are ready.
* **Reliability over Unreliable Channels:** Guarantees 100% data integrity over a data link characterized by unspecified packet loss, constant latency, and limited bandwidth.
* **Sliding Window Protocol:** Implements custom sliding windows on **both the sender and the receiver** sides to optimize network utilization and throughput according to latency and transmission speed.
* **Custom Protocol Headers:** Defines a custom segment structure encapsulating sequence numbers, acknowledgments (ACKs), flags, and payload data.
* **Unidirectional Data Flow with Control Feedback:** The Client acts as the data sender, while the Server receives the data and continuously transmits control segments (ACKs) back to the sender to confirm successful reception or request retransmissions.

## ⚙️ Protocol Design & Architecture

### Segment Structure
The unit of transmission is a **Segment**, encapsulated within a standard UDP datagram. The custom header includes:
- `Sequence Number`: To order data and detect missing segments.
- `Acknowledgment (ACK) Number`: Sent by the server to confirm receipt.
- `Flags`: Used for connection management (e.g., SYN, FIN, ACK).
- `Payload`: The actual file data being transferred.

### Flow Control & Error Recovery
To utilize the data link optimally:
- The **Sender Window** limits the number of unacknowledged segments in flight, preventing network congestion and receiver overwhelm.
- The **Receiver Window** buffers out-of-order packets and ensures that data is written to the output file sequentially.
- **Retransmission Mechanism:** The sender relies on timeouts and duplicate ACKs (similar to TCP's Go-Back-N or Selective Repeat paradigms) to detect and retransmit lost segments.

## 🛠️ Environment & Tools
* **Language:** C / C++
* **Network Stack:** POSIX Sockets, UDP (Datagram Sockets)
* **Concepts:** Flow Control, Sliding Windows, Congestion Avoidance, TCP-like reliability, Sequence/ACK mechanisms.

## 🏃‍♂️ How to Run

1. Compile the source code using the provided Makefile:
```bash
make
