#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <stdio.h>
#include <cstring>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int g_recv_buffer_bytes = 9 * 1024; 

int recv_data(int conn_id, char *buffer, int len)
{
	int size = 0;
	pthread_mutex_lock(&cons[conn_id]->con_lock);

	/* We will write code here as to not have sync problems with recv_handler */

	struct connection *con = cons[conn_id];
	
	int available_bytes = con->recv_write_pos - con->recv_read_pos;
	
	if (available_bytes > 0) {
		if (len < available_bytes) {
			size = len;
		} else {
			size = available_bytes;
		}
		memcpy(buffer, con->recv_buffer + con->recv_read_pos, size);
		con->recv_read_pos = con->recv_read_pos + size;
		
		int remaining = con->recv_write_pos - con->recv_read_pos;
		if (remaining > 0) {
			memmove(con->recv_buffer, con->recv_buffer + con->recv_read_pos, remaining);
		}
		con->recv_write_pos = remaining;
		con->recv_read_pos = 0;
		
		poli_tcp_ctrl_hdr ack_pkt;
		ack_pkt.protocol_id = POLI_PROTOCOL_ID;
		ack_pkt.conn_id = conn_id;
		ack_pkt.type = 5; 
		ack_pkt.ack_num = con->expected_seq_num; 
		
		int space_left_bytes = con->recv_buffer_size - con->recv_write_pos - (con->out_of_order_packets.size() * MAX_DATA_SIZE);
		if (space_left_bytes < 0) space_left_bytes = 0;
		ack_pkt.recv_window = space_left_bytes / MAX_DATA_SIZE; 
		if (ack_pkt.recv_window == 0) ack_pkt.recv_window = 1; 
		
		sendto(con->sockfd, &ack_pkt, sizeof(ack_pkt), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
		
		pthread_mutex_unlock(&cons[conn_id]->con_lock);
		return size;
	} else {
		pthread_mutex_unlock(&cons[conn_id]->con_lock);
		usleep(1000); 
		return 0;
	}
}

void *receiver_handler(void *arg)
{
	char segment[MAX_SEGMENT_SIZE];
	int res;
	DEBUG_PRINT("Starting recviver handler\n");

	while (1) {
		if (cons.size() == 0) continue;

		int conn_id = -1;
		do {
			res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
			if (res == -14) usleep(1000); 
		} while(res == -14);

		if (res > 0) {
			pthread_mutex_lock(&cons[conn_id]->con_lock);
			struct connection *con = cons[conn_id];
			poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;
			
			if (hdr->type == 4) {
				uint16_t seq = hdr->seq_num;
				int data_len = hdr->len;
				
				int16_t diff = (int16_t)(seq - con->expected_seq_num);
				
				if (diff == 0) {
					if (con->recv_write_pos + data_len > con->recv_buffer_size) {
						int remaining = con->recv_write_pos - con->recv_read_pos;
						if (remaining > 0) memmove(con->recv_buffer, con->recv_buffer + con->recv_read_pos, remaining);
						con->recv_write_pos = remaining;
						con->recv_read_pos = 0;
					}

					if (con->recv_write_pos + data_len <= con->recv_buffer_size) {
						memcpy(con->recv_buffer + con->recv_write_pos, segment + sizeof(poli_tcp_data_hdr), data_len);
						con->recv_write_pos = con->recv_write_pos + data_len;
						con->out_of_order_packets.erase(seq);
						con->expected_seq_num++;
						
						while (con->out_of_order_packets.count(con->expected_seq_num)) {
							struct in_flight_packet pkt = con->out_of_order_packets[con->expected_seq_num];
							int pkt_data_len = pkt.len - sizeof(poli_tcp_data_hdr);
							
							if (con->recv_write_pos + pkt_data_len > con->recv_buffer_size) {
								int remaining = con->recv_write_pos - con->recv_read_pos;
								if (remaining > 0) memmove(con->recv_buffer, con->recv_buffer + con->recv_read_pos, remaining);
								con->recv_write_pos = remaining;
								con->recv_read_pos = 0;
							}
							
							if (con->recv_write_pos + pkt_data_len <= con->recv_buffer_size) {
								memcpy(con->recv_buffer + con->recv_write_pos, pkt.data + sizeof(poli_tcp_data_hdr), pkt_data_len);
								con->recv_write_pos = con->recv_write_pos + pkt_data_len;
								con->out_of_order_packets.erase(con->expected_seq_num);
								con->expected_seq_num++;
							} else {
								break; 
							}
						}
					}
				} 
				else if (diff > 0) {
					struct in_flight_packet pkt;
					pkt.len = res;
					memcpy(pkt.data, segment, res);
					con->out_of_order_packets[seq] = pkt; 
				}
				
				poli_tcp_ctrl_hdr ack_pkt;
				ack_pkt.protocol_id = POLI_PROTOCOL_ID;
				ack_pkt.conn_id = conn_id;
				ack_pkt.type = 5; 
				ack_pkt.ack_num = con->expected_seq_num; 
				
				int bytes_in_buffer = con->recv_write_pos - con->recv_read_pos;
				int out_of_order_bytes = con->out_of_order_packets.size() * MAX_DATA_SIZE;
				int space_left_bytes = con->recv_buffer_size - bytes_in_buffer - out_of_order_bytes;
				if (space_left_bytes < 0) space_left_bytes = 0;
				
				ack_pkt.recv_window = space_left_bytes / MAX_DATA_SIZE; 
				if (ack_pkt.recv_window == 0) ack_pkt.recv_window = 1; 
				
				sendto(con->sockfd, &ack_pkt, sizeof(ack_pkt), 0, (struct sockaddr*)&con->servaddr, sizeof(con->servaddr));
			}
			pthread_mutex_unlock(&cons[conn_id]->con_lock);
		}
	}
	return NULL;
}

int wait4connect(uint32_t ip, uint16_t port)
{
	/* TODO: Implement the Three Way Handshake on the receiver part. This blocks
	 * until a connection is established. */

	struct connection *con = new connection();
	int conn_id = cons.size();

	int listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	int opt = 1;
	setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in listen_addr;
	listen_addr.sin_family = AF_INET;
	listen_addr.sin_port = port;
	listen_addr.sin_addr.s_addr = ip;
	bind(listen_sockfd, (struct sockaddr*)&listen_addr, sizeof(listen_addr));

	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);
	char recv_buf[MAX_SEGMENT_SIZE];

	recvfrom(listen_sockfd, recv_buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_len);
	poli_tcp_ctrl_hdr* syn_pkt = (poli_tcp_ctrl_hdr*)recv_buf;

	if (syn_pkt->type == 1) {
		con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
		struct sockaddr_in new_addr;
		new_addr.sin_family = AF_INET;
		new_addr.sin_port = htons(0); 
		new_addr.sin_addr.s_addr = INADDR_ANY;
		bind(con->sockfd, (struct sockaddr*)&new_addr, sizeof(new_addr));

		socklen_t new_addr_len = sizeof(new_addr);
		getsockname(con->sockfd, (struct sockaddr*)&new_addr, &new_addr_len);
		uint16_t new_port = new_addr.sin_port;

		char send_buf[MAX_SEGMENT_SIZE];
		poli_tcp_ctrl_hdr* syn_ack_pkt = (poli_tcp_ctrl_hdr*)send_buf;
		syn_ack_pkt->protocol_id = POLI_PROTOCOL_ID;
		syn_ack_pkt->conn_id = conn_id;
		syn_ack_pkt->type = 2; 
		syn_ack_pkt->recv_window = g_recv_buffer_bytes / MAX_DATA_SIZE; 

		memcpy(send_buf + sizeof(poli_tcp_ctrl_hdr), &new_port, sizeof(uint16_t));

		sendto(listen_sockfd, send_buf, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0, (struct sockaddr*)&client_addr, client_len);

		struct pollfd fds[2];
		fds[0].fd = listen_sockfd; 
		fds[0].events = POLLIN;
		fds[1].fd = con->sockfd;   
		fds[1].events = POLLIN;

		while (1) {
			int ret = poll(fds, 2, 100); 
			if (ret == 0) {
				sendto(listen_sockfd, send_buf, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0, (struct sockaddr*)&client_addr, client_len);
			} else if (ret > 0) {
				if (fds[0].revents & POLLIN) {
					recvfrom(listen_sockfd, recv_buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr*)&client_addr, &client_len);
					sendto(listen_sockfd, send_buf, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0, (struct sockaddr*)&client_addr, client_len);
				}
				
				if (fds[1].revents & POLLIN) {
					poli_tcp_ctrl_hdr* ack_pkt = (poli_tcp_ctrl_hdr*)recv_buf;
					int n = recvfrom(con->sockfd, recv_buf, MAX_SEGMENT_SIZE, MSG_PEEK, (struct sockaddr*)&client_addr, &client_len);
					
					if (n > 0) {
						if (ack_pkt->type == 3) { 
							recvfrom(con->sockfd, recv_buf, MAX_SEGMENT_SIZE, 0, NULL, NULL);
							break; 
						} else if (ack_pkt->type == 4) { 
							break; 
						} else {
							recvfrom(con->sockfd, recv_buf, MAX_SEGMENT_SIZE, 0, NULL, NULL); 
						}
					}
				}
			}
		}

		con->servaddr = client_addr;
	}

	close(listen_sockfd);

	con->expected_seq_num = 0;
	con->recv_buffer_size = g_recv_buffer_bytes; 
	con->recv_window = con->recv_buffer_size / MAX_DATA_SIZE;
	con->recv_buffer = (char*)malloc(con->recv_buffer_size);
	con->recv_read_pos = 0;
	con->recv_write_pos = 0;

	/* Since we can have multiple connection, we want to know if data is available
	   on the socket used by a given connection. We use POLL for this */
	data_fds[fdmax].fd = con->sockfd;    
	data_fds[fdmax].events = POLLIN; 

	/* This creates a timer and sets it to trigger every 1 sec. We use this
	   to know if a timeout has happend on a connection */   
	timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
	timer_fds[fdmax].events = POLLIN;    
	struct itimerspec spec;     
	spec.it_value.tv_sec = 1;    
	spec.it_value.tv_nsec = 0;    
	spec.it_interval.tv_sec = 1;    
	spec.it_interval.tv_nsec = 0;    
	timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
	fdmax++;    

	pthread_mutex_init(&con->con_lock, NULL);
	cons.insert({conn_id, con});

	DEBUG_PRINT("Connection established!");

	return conn_id; 
}

void init_receiver(int recv_buffer_bytes)
{
	g_recv_buffer_bytes = recv_buffer_bytes; 
	
	pthread_t thread1;
	int ret;

	/* TODO: Create the connection socket and bind it to 8031 */

	ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
	assert(ret == 0);
}