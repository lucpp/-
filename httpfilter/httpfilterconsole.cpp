
#include "stdafx.h"

#include <winsock2.h>
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <windivert.h>
#pragma comment(lib, "winDivert.lib")
#pragma comment(lib, "WS2_32.lib")


#include "http_parser.h"


#define MAXBUF 0xFFFF
#define MAXURL 4096

http_parser parser;
bool temp_header_complete = false;
bool temp_is_hostheader = false;
char temp_host[1024];

int port = 80;
bool loopback = false;
char target_url[1024];
char new_host_url[1024];


typedef struct
{
	WINDIVERT_IPHDR  ip;
	WINDIVERT_TCPHDR tcp;
} PACKET, *PPACKET;
typedef struct
{
	PACKET header;
	UINT8 data[];
} DATAPACKET, *PDATAPACKET;


static void PacketInit(PPACKET packet);

static BOOL HttpRequestPayloadMatch(char *data,
	UINT16 len);



int my_url_callback(http_parser*, const char *at, size_t length)
{
	return 0;
}

int my_header_field_callback(http_parser*, const char *at, size_t length)
{
	if (_strnicmp(at, "host",4) == 0)
		temp_is_hostheader = true;
	else
		temp_is_hostheader = false;
	return 0;
}

int my_header_value_callback(http_parser*, const char *at, size_t length)
{
	if (temp_is_hostheader == true) {
		if (length >= 1024)
			length = 1023;
		memcpy(temp_host, at, length);
		temp_host[length] = '\0';
		temp_is_hostheader = false;
	}
	return 0;
}

int  my_headers_complete_callback(http_parser*)
{
	temp_header_complete = true;
	return 0;
}

int __cdecl main(int argc, char **argv)
{
	HANDLE handle;
	WINDIVERT_ADDRESS addr;
	UINT8 packet[MAXBUF];
	UINT packet_len;
	PWINDIVERT_IPHDR ip_header;
	PWINDIVERT_TCPHDR tcp_header;
	PVOID payload;
	UINT payload_len;
	PACKET reset0;
	PPACKET reset = &reset0;
	PACKET finish0;
	PPACKET finish = &finish0;
	PDATAPACKET blockpage;
	UINT16 blockpage_len;

	unsigned i;
	INT16 priority = 404;       // Arbitrary.

	if (argc != 5)
	{
		fprintf(stderr, "usage: %s 80 www.qq.com  www.my_host.com  false\n",
			argv[0]);
		exit(EXIT_FAILURE);
	}
	port = atoi(argv[1]);
	strcpy(target_url, argv[2]);
	strcpy(new_host_url, argv[3]);

	if (_strnicmp(argv[4], "true", 4) == 0) {
		loopback = true;
	}
	else {
		loopback = false;
	}

	printf("%s \n port:%d \n target_url:%s \n new_host_url:%s \n local test:%s \n ",
		argv[0], port, target_url, new_host_url, loopback ? "true" : "false");


	char* responseData = (char*)malloc(1024*2);
	responseData[0] = '\0';
	strcat(responseData, "HTTP/1.1 302 Found\r\nContent-Type: text/html; charset=utf-8\r\nLocation: http://");
	strcat(responseData, new_host_url);
	strcat(responseData, "\r\nDate: Mon, 09 Jul 2018 06:27:33 GMT\r\nContent-Length:3\r\n\r\n302");

	// Initialize the pre-frabricated packets:
	blockpage_len = sizeof(DATAPACKET) + strlen(responseData);
	blockpage = (PDATAPACKET)malloc(blockpage_len);
	if (blockpage == NULL)
	{
		fprintf(stderr, "error: memory allocation failed\n");
		exit(EXIT_FAILURE);
	}
	PacketInit(&blockpage->header);
	blockpage->header.ip.Length = htons(blockpage_len);
	blockpage->header.tcp.SrcPort = htons(port);
	blockpage->header.tcp.Psh = 1;
	blockpage->header.tcp.Ack = 1;
	memcpy(blockpage->data, responseData, strlen(responseData));
	PacketInit(reset);
	reset->tcp.Rst = 1;
	reset->tcp.Ack = 1;
	PacketInit(finish);
	finish->tcp.Fin = 1;
	finish->tcp.Ack = 1;

	char filter[1024];
	 sprintf(filter, "outbound && "
		 "%s loopback && "
		 "ip && "
		 "tcp.DstPort == %d && "
		 "tcp.PayloadLength > 0",loopback?"":"!", port);


	 //	"outbound && "              // Outbound traffic only
		//"!loopback && "             // No loopback traffic
		//"ip && "                    // Only IPv4 supported
		//"tcp.DstPort == 80 && "     // HTTP (port 80) only
		//"tcp.PayloadLength > 0",    // TCP data packets only
	// Open the Divert device:
	handle = WinDivertOpen(
		filter,   
		WINDIVERT_LAYER_NETWORK, priority, 0
	);
	if (handle == INVALID_HANDLE_VALUE)
	{
		fprintf(stderr, "error: failed to open the WinDivert device (%d)\n",
			GetLastError());
		exit(EXIT_FAILURE);
	}

	printf("--------Open Success,Recving----------\n");

	// Main loop:
	while (TRUE)
	{
		if (!WinDivertRecv(handle, packet, sizeof(packet), &addr, &packet_len))
		{
			fprintf(stderr, "warning: failed to read packet (%d)\n",
				GetLastError());
			continue;
		}

		if (!WinDivertHelperParsePacket(packet, packet_len, &ip_header, NULL,
			NULL, NULL, &tcp_header, NULL, &payload, &payload_len) ||
			!HttpRequestPayloadMatch( (char*)payload, (UINT16)payload_len))
		{
			// Packet does not match the blacklist; simply reinject it.
			if (!WinDivertSend(handle, packet, packet_len, &addr, NULL))
			{
				fprintf(stderr, "warning: failed to reinject packet (%d)\n",
					GetLastError());
			}
			continue;
		}

		// The URL matched the blacklist; we block it by hijacking the TCP
		// connection.

		// (1) Send a TCP RST to the server; immediately closing the
		//     connection at the server's end.
		reset->ip.SrcAddr = ip_header->SrcAddr;
		reset->ip.DstAddr = ip_header->DstAddr;
		reset->tcp.SrcPort = tcp_header->SrcPort;
		reset->tcp.DstPort = htons(port);
		reset->tcp.SeqNum = tcp_header->SeqNum;
		reset->tcp.AckNum = tcp_header->AckNum;
		WinDivertHelperCalcChecksums((PVOID)reset, sizeof(PACKET), &addr, 0);
		if (!WinDivertSend(handle, (PVOID)reset, sizeof(PACKET), &addr, NULL))
		{
			fprintf(stderr, "warning: failed to send reset packet (%d)\n",
				GetLastError());
		}

		// (2) Send the blockpage to the browser:
		blockpage->header.ip.SrcAddr = ip_header->DstAddr;
		blockpage->header.ip.DstAddr = ip_header->SrcAddr;
		blockpage->header.tcp.DstPort = tcp_header->SrcPort;
		blockpage->header.tcp.SeqNum = tcp_header->AckNum;
		blockpage->header.tcp.AckNum =
			htonl(ntohl(tcp_header->SeqNum) + payload_len);
		addr.Direction = !addr.Direction;     // Reverse direction.
		WinDivertHelperCalcChecksums((PVOID)blockpage, blockpage_len, &addr, 0);
		if (!WinDivertSend(handle, (PVOID)blockpage, blockpage_len, &addr,
			NULL))
		{
			fprintf(stderr, "warning: failed to send block page packet (%d)\n",
				GetLastError());
		}

		// (3) Send a TCP FIN to the browser; closing the connection at the 
		//     browser's end.
		finish->ip.SrcAddr = ip_header->DstAddr;
		finish->ip.DstAddr = ip_header->SrcAddr;
		finish->tcp.SrcPort = htons(port);
		finish->tcp.DstPort = tcp_header->SrcPort;
		finish->tcp.SeqNum =
			htonl(ntohl(tcp_header->AckNum) + strlen(responseData));
		finish->tcp.AckNum =
			htonl(ntohl(tcp_header->SeqNum) + payload_len);
		WinDivertHelperCalcChecksums((PVOID)finish, sizeof(PACKET), &addr, 0);
		if (!WinDivertSend(handle, (PVOID)finish, sizeof(PACKET), &addr, NULL))
		{
			fprintf(stderr, "warning: failed to send finish packet (%d)\n",
				GetLastError());
		}
	}
}

static void PacketInit(PPACKET packet)
{
	memset(packet, 0, sizeof(PACKET));
	packet->ip.Version = 4;
	packet->ip.HdrLength = sizeof(WINDIVERT_IPHDR) / sizeof(UINT32);
	packet->ip.Length = htons(sizeof(PACKET));
	packet->ip.TTL = 64;
	packet->ip.Protocol = IPPROTO_TCP;
	packet->tcp.HdrLength = sizeof(WINDIVERT_TCPHDR) / sizeof(UINT32);
}


static BOOL HttpRequestPayloadMatch(char *data, UINT16 len)
{

	http_parser_init(&parser, HTTP_REQUEST);
	http_parser_settings settings;
	memset(&settings,0, sizeof(settings));
	settings.on_url = my_url_callback;
	settings.on_header_field = my_header_field_callback;
	settings.on_header_value = my_header_value_callback;
	settings.on_headers_complete = my_headers_complete_callback;

	temp_header_complete = false;
	temp_is_hostheader = false;
	temp_host[0] = '\0';
	size_t nparsed = http_parser_execute(&parser, &settings, data, len);

	if (temp_header_complete == true) {
		if (_stricmp(target_url, temp_host)==0) {

			printf("%.*s \n", len, data);
			printf("Successful match!\n");
			return true;
		}
	}
	return false;
}
