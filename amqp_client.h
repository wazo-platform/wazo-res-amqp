/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2019 The Wazo Authors  (see the AUTHORS file)
 *
 * Nicolaos Ballas <nballas@wazo.io>
 *
 */

#ifndef _ASTERISK_AMQP_CLIENT_H
#define _ASTERISK_AMQP_CLIENT_H

#include "asterisk.h"
#include "asterisk/module.h"
#include "asterisk/amqp.h"
#include "amqp/internal.h"

#include "amqp_message.h"

ast_cond_t start_cond;
ast_mutex_t start_lock;
extern int client_start_flag;

struct ast_amqp_connection
{
	amqp_connection_state_t state;
	char name[];
};

struct amqp_recv_message_loop_args;

typedef int (*amqp_message_handler)(void*);

void stop_recv_loop(void);

void add_client_connection(const char *connection_name, amqp_connection_state_t state, const char *exchange, amqp_bytes_t queuename);

void remove_client_connection(const char *connection_name);

struct amqp_recv_message_loop_args *get_args_for_connection(const char *connection_name);

amqp_connection_state_t loop_args_connection_state(struct amqp_recv_message_loop_args *args);

amqp_bytes_t loop_args_queuename(struct amqp_recv_message_loop_args *args);

const char *loop_args_connection_exchange(struct amqp_recv_message_loop_args *args);

int init_amqp_client(void);

int amqp_start_client(const char *connection_name, const char *exchange);

int reload_amqp_client(void);

void destroy_amqp_client(void);

int respond_to_amqp_message(struct amqp_message *msg);

int amqp_register_callback(const char *connection_name, const char *exchange, const char* routing_key, amqp_message_handler handler);

void amqp_recv_message_loop(struct amqp_recv_message_loop_args *args);

#endif //_ASTERISK_AMQP_CLIENT_H
