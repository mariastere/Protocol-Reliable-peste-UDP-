#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int send_data(int conn_id, char *buffer, int len)
{
	int bytes_to_send = 0;
	pthread_mutex_lock(&cons[conn_id]->con_lock);
	struct connection *con = cons[conn_id];

	int16_t pachete_in_zbor = (int16_t)(con->next_seq_num - con->base_seq_num);
	
	if (pachete_in_zbor >= con->recv_window) {
		pthread_mutex_unlock(&con->con_lock);
		usleep(1000); 
		return -1; 
	}

	if (len > MAX_DATA_SIZE) {
		bytes_to_send = MAX_DATA_SIZE;
	} else {
		bytes_to_send = len;
	}

	struct in_flight_packet pkt;
	pkt.len = sizeof(poli_tcp_data_hdr) + bytes_to_send;
	poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)pkt.data;
	hdr->protocol_id = POLI_PROTOCOL_ID;
	hdr->conn_id = conn_id;
	hdr->type = 4; 
	hdr->seq_num = con->next_seq_num;
	hdr->len = bytes_to_send;

	memcpy(pkt.data + sizeof(poli_tcp_data_hdr), buffer, bytes_to_send);
	con->unacked_packets[con->next_seq_num] = pkt;

	sendto(con->sockfd, pkt.data, pkt.len, 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
	con->next_seq_num++;

	pthread_mutex_unlock(&cons[conn_id]->con_lock);
	return bytes_to_send;
}

void *sender_handler(void *arg)
{
	int res = 0;
	char buf[MAX_SEGMENT_SIZE];

	while (1) {
		if (cons.size() == 0) {
			continue;
		}
		int conn_id = -1;
		do {
			res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
			if (res == -14) usleep(1000); 
		} while(res == -14);

		pthread_mutex_lock(&cons[conn_id]->con_lock);
		struct connection *con = cons[conn_id];

		if (res > 0) {
			poli_tcp_ctrl_hdr *ack_pkt = (poli_tcp_ctrl_hdr *)buf;
			
			if (ack_pkt->type == 5) {
				uint16_t ack_num = ack_pkt->ack_num;
				con->recv_window = ack_pkt->recv_window; 

				int16_t diff = (int16_t)(ack_num - con->base_seq_num);
				if (diff > 0) {
					for (int16_t i = 0; i < diff; i++) {
						con->unacked_packets.erase((uint16_t)(con->base_seq_num + i));
					}
					con->base_seq_num = ack_num;
				}
			} 
			else if (ack_pkt->type == 2) {
				// clientul retrimite ACK daca s a pierdut pe drum
				poli_tcp_ctrl_hdr ack_reply;
				ack_reply.protocol_id = POLI_PROTOCOL_ID;
				ack_reply.conn_id = conn_id;
				ack_reply.type = 3; 
				sendto(con->sockfd, &ack_reply, sizeof(ack_reply), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
			}
		} 
		else if (res == -1) {
			if (!con->unacked_packets.empty()) {
				for (auto const& [seq, pkt] : con->unacked_packets) {
					sendto(con->sockfd, pkt.data, pkt.len, 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
				}
			}
		}
		pthread_mutex_unlock(&cons[conn_id]->con_lock);
	}
	return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
	/* Implement the sender part of the Three Way Handshake. Blocks
	until the connection is established */

	struct connection *con = new connection();
	int conn_id = 0;
	con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

	con->servaddr.sin_family = AF_INET;
	con->servaddr.sin_port = port;
	con->servaddr.sin_addr.s_addr = ip;

	poli_tcp_ctrl_hdr syn_pkt;
	syn_pkt.protocol_id = POLI_PROTOCOL_ID;
	syn_pkt.conn_id = conn_id;
	syn_pkt.type = 1;

	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 100000;
	setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

	char recv_buf[MAX_SEGMENT_SIZE];
	struct sockaddr_in recv_addr;
	socklen_t addr_len = sizeof(recv_addr);
	poli_tcp_ctrl_hdr* syn_ack_pkt = (poli_tcp_ctrl_hdr*) recv_buf;

	int n = -1;
	syn_ack_pkt->type = 0;
	while (n < 0 || syn_ack_pkt->type != 2) {
		sendto(con->sockfd, &syn_pkt, sizeof(syn_pkt), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
		n = recvfrom(con->sockfd, recv_buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr*) &recv_addr, &addr_len);
	}

	tv.tv_usec = 0;
	setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

	uint16_t new_port = *(uint16_t*)(recv_buf + sizeof(poli_tcp_ctrl_hdr));
	con->servaddr.sin_port = new_port;
	con->recv_window = syn_ack_pkt->recv_window;

	poli_tcp_ctrl_hdr ack_pkt;
	ack_pkt.protocol_id = POLI_PROTOCOL_ID;
	ack_pkt.conn_id = conn_id;
	ack_pkt.type = 3;
	sendto(con->sockfd, &ack_pkt, sizeof(ack_pkt), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));

	con->base_seq_num = 0;
	con->next_seq_num = 0;

	/* Since we can have multiple connection, we want to know if data is available
	   on the socket used by a given connection. We use POLL for this */
	data_fds[fdmax].fd = con->sockfd;    
	data_fds[fdmax].events = POLLIN;

	/* This creates a timer and sets it to trigger every 1 sec. We use this
	   to know if a timeout has happend on our connection */
	timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
	timer_fds[fdmax].events = POLLIN;    
	struct itimerspec spec;     
	spec.it_value.tv_sec = 0;    
	spec.it_value.tv_nsec = 30000000; 
	spec.it_interval.tv_sec = 0;    
	spec.it_interval.tv_nsec = 30000000;    
	timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
	fdmax++;

	pthread_mutex_init(&con->con_lock, NULL);
	cons.insert({conn_id, con});

	return 0;
}

void init_sender(int speed, int delay)
{
	pthread_t thread1;
	int ret;

	/* Create a thread that will*/
	ret = pthread_create( &thread1, NULL, sender_handler, NULL);
	assert(ret == 0);
}