/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2019 The Wazo Authors  (see the AUTHORS file)
 *
 */

#include "amqp_client.h"

#define MAX_LOOPS 31
#define MAX_STR_ARG_SIZE 80

AST_MUTEX_DEFINE_STATIC(client_lock);

static int stop_recv_loop_flag = 0;
static int num_loop_threads = 0;
static pthread_t recv_thread;

void stop_recv_loop(void) {
	ast_debug(3, "stopping reception loop\n");
	stop_recv_loop_flag = 1;
}

struct routing_key_handlers {
	char routing_key[MAX_STR_ARG_SIZE];
	amqp_message_handler handler;
};

struct amqp_recv_message_loop_args {
	amqp_connection_state_t connection_state;
	char exchange[MAX_STR_ARG_SIZE];
	struct routing_key_handlers handlers[100];
	amqp_bytes_t queuename;

};

struct loop_args_container_element {
	int occupied;
	char connection_name[MAX_STR_ARG_SIZE];
	struct amqp_recv_message_loop_args *args;
};

static struct loop_args_container_element* loop_args_container[MAX_LOOPS];

void add_client_connection(const char *connection_name, amqp_connection_state_t state, const char *exchange, amqp_bytes_t queuename)
{
	ast_mutex_lock(&client_lock);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (!loop_args_container[i]->occupied) {
			loop_args_container[i]->occupied = 1;
			strcpy(loop_args_container[i]->connection_name, connection_name);
			loop_args_container[i]->args->connection_state = state;
			strcpy(loop_args_container[i]->args->exchange, exchange);
			loop_args_container[i]->args->queuename = amqp_bytes_malloc_dup(queuename);
			memcpy(loop_args_container[i]->args->queuename.bytes, queuename.bytes, queuename.len);
			loop_args_container[i]->args->queuename.len = queuename.len;
			break;
		}
	}

	ast_mutex_unlock(&client_lock);
}

int amqp_register_callback(const char *connection_name, const char *exchange, const char* routing_key, amqp_message_handler handler)
{
	ast_log(LOG_ERROR, "registering callback for key: %s\n", routing_key);
	ast_mutex_lock(&client_lock);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (!loop_args_container[i]->occupied) {
			continue;
		}

		if (!strcmp(connection_name, connection_name)) {
			if (!strcmp(loop_args_container[i]->args->exchange, exchange)) {
				for (int j = 0; j < 100; ++j) { // todo fix hard coded size
					if (loop_args_container[i]->args->handlers[j].handler == NULL) {
						loop_args_container[i]->args->handlers[j].handler = handler;
						strcpy(loop_args_container[i]->args->handlers[j].routing_key, routing_key);
						ast_log(LOG_ERROR, "registered callback for key: %s\n", loop_args_container[i]->args->handlers[j].routing_key);
						ast_mutex_unlock(&client_lock);
						amqp_queue_bind_ok_t *ok = amqp_queue_bind(
							loop_args_container[i]->args->connection_state,
							/*channel*/ 1,
							amqp_cstring_bytes("this queue")/*loop_args_container[i]->args->queuename*/,
							amqp_cstring_bytes(exchange),
							amqp_cstring_bytes(routing_key),
							amqp_empty_table
						);
						// TODO: Find out what the following line is supposed to do. It looks like a left over logger or print
						(amqp_get_rpc_reply(loop_args_container[i]->args->connection_state), "Consuming");
						if (ok == NULL) {
							ast_log(LOG_ERROR, "unable to bind queue for key %s\n", loop_args_container[i]->args->handlers[j].routing_key);
						} else {
							ast_log(LOG_ERROR, "OK bound queue for key %s\n", loop_args_container[i]->args->handlers[j].routing_key);
						}
						return 0;
					}
				}
			}
		}
	}
	ast_mutex_unlock(&client_lock);
	return 0;
}

void remove_client_connection(const char *connection_name)
{
	ast_mutex_lock(&client_lock);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (loop_args_container[i]->occupied && !strcmp(loop_args_container[i]->connection_name, connection_name)) {
			loop_args_container[i]->occupied = 0;
			break;
		}
	}
	ast_mutex_unlock(&client_lock);
}

static void run_amqp_receiver(amqp_connection_state_t conn)
{
	while (!stop_recv_loop_flag) {
		amqp_rpc_reply_t ret;
		amqp_envelope_t envelope;

		amqp_maybe_release_buffers(conn);
		ast_log(LOG_ERROR, "waiting...\n");
		ret = amqp_consume_message(conn, &envelope, NULL, 0);
		struct amqp_message *message = NULL;

		if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
			message = amqp_message_create_from_envelope(&envelope);

			if (!message) {
				ast_log(LOG_WARNING, "error creating AMQP message from envelope\n");
				goto after;
			}

			ast_log(LOG_WARNING, "got message...: %s\n", amqp_message_body(message));

			if (ret.reply_type == AMQP_RESPONSE_NORMAL) {
				int res = respond_to_amqp_message(message);
				if (res != 0) {
					ast_log(LOG_ERROR, "error during AMQP API call\n");
				}
			}
		} else if (ret.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
			// ast_log(LOG_ERROR, "error:%s\n", amqp_error_string(ret.library_error)); // use amqp_error_string2
			stop_recv_loop_flag = 1;
			// exit(0);
		}
after:
		amqp_message_destroy(message);
	}
}

struct amqp_recv_message_loop_args *get_args_for_connection(const char *connection_name)
{
	ast_mutex_lock(&client_lock);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (!loop_args_container[i]->occupied) {
			continue;
		}
		if (!strcmp(loop_args_container[i]->connection_name, connection_name)) {
			ast_mutex_unlock(&client_lock);
			return loop_args_container[i]->args;
		}
	}
	ast_mutex_unlock(&client_lock);
	return NULL;
}

amqp_connection_state_t loop_args_connection_state(struct amqp_recv_message_loop_args *args)
{
	return args->connection_state;
}

amqp_bytes_t loop_args_queuename(struct amqp_recv_message_loop_args *args)
{
	return args->queuename;
}

const char *loop_args_connection_exchange(struct amqp_recv_message_loop_args *args)
{
	return args->exchange;
}

int init_amqp_client(void)
{
	ast_mutex_init(&client_lock);
	ast_mutex_lock(&client_lock);
	struct loop_args_container_element *element = NULL;
	for (int i = 0; i < MAX_LOOPS; ++i) {
		element = ast_malloc(sizeof(struct loop_args_container_element));
		if (!element) {
			ast_log(LOG_ERROR, "memory allocation error\n");
			goto on_error;
		}
		element->args = ast_malloc(sizeof(struct amqp_recv_message_loop_args));
		if (!element->args) {
			ast_log(LOG_ERROR, "memory allocation error\n");
			goto on_error;
		}

		loop_args_container[i] = element;
		loop_args_container[i]->occupied = 0;
	}
	ast_mutex_unlock(&client_lock);
	return 0;

on_error:
	ast_free(element);
	ast_mutex_unlock(&client_lock);
	destroy_amqp_client();
	return -1;
}

void amqp_recv_message_loop(struct amqp_recv_message_loop_args *args)
{
	ast_log(LOG_ERROR, "recv loop start...\n");
	ast_log(LOG_ERROR, "waiting for %p\n", &client_start_flag);
	ast_mutex_lock(&start_lock);
	while (!client_start_flag) {
		ast_log(LOG_ERROR, "inside wait loop\n");

		// ast_cond_wait(&start_cond, &start_lock);

		ast_log(LOG_ERROR, "after wait...\n");
	}
	ast_mutex_unlock(&start_lock);

	amqp_connection_state_t connection_state = loop_args_connection_state(args);
	amqp_bytes_t queuename = loop_args_queuename(args);

	if (queuename.bytes == NULL) {
		ast_log(LOG_ERROR, "queue name error\n");
		return;
	}
	ast_log(LOG_ERROR, "consuming...\n");
	amqp_basic_consume(connection_state, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
	ast_log(LOG_ERROR, "consumed\n");
	run_amqp_receiver(connection_state);

	return;
}

static void *worker(void* args) {
	struct amqp_recv_message_loop_args *lp = args;
	amqp_recv_message_loop(lp);
	return NULL;
}

/**
 * Start an AMQP client on a separate thread
 * @param connection_name The AMQP connection name
 * @param exchange The AMQP exchange name
 * @return 0 on success
 */
int amqp_start_client(const char *connection_name, const char *exchange)
{
	if (num_loop_threads >= 31) {
		ast_log(LOG_ERROR, "exceeded maximum number of threads\n");
		return -1;
	}
	ast_log(LOG_ERROR, "starting client...\n");

	struct ast_amqp_connection *connection = ast_amqp_get_connection(connection_name);
	if (connection) {
		ast_debug(1, "starting client thread... exchange: %s\n", exchange);
		ast_log(LOG_ERROR, "starting client thread... exchange: %s\n", exchange);
		amqp_queue_declare_ok_t *r = amqp_queue_declare(connection->state, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
		amqp_get_rpc_reply(connection->state);
		amqp_bytes_t queuename = amqp_bytes_malloc_dup(r->queue);
		if (queuename.bytes == NULL) {
			ast_log(LOG_ERROR, "queue name error!\n");
			return -1;
		}

		add_client_connection(connection_name, connection->state, exchange, queuename);

		struct amqp_recv_message_loop_args *args = get_args_for_connection(connection_name);
		if (!args) {
			ast_log(LOG_ERROR, "unable find arguments no such connection: %s\n", connection_name);
			return -1;
		}
		ast_log(LOG_ERROR, "launching worker\n");
		if(pthread_create(&recv_thread, NULL, worker, args)) {
			ast_log(LOG_ERROR, "unable to launch thread\n");
			return -1;
		}
		num_loop_threads++;
	} else {
		ast_log(LOG_ERROR, "no such connection: %s\n", connection_name);
		return -1;
	}

	return 0;
}

int reload_amqp_client(void)
{
	ast_mutex_lock(&client_lock);

	for (int i = 0; i < MAX_LOOPS; ++i) {
		loop_args_container[i]->occupied = 0;
		loop_args_container[i]->args->connection_state = 0;

		memset(loop_args_container[i]->connection_name, 0, MAX_STR_ARG_SIZE);
		memset(loop_args_container[i]->args->exchange, 0, MAX_STR_ARG_SIZE);
	}

	ast_mutex_unlock(&client_lock);
	return 0;
}

void destroy_amqp_client(void)
{
	pthread_join(recv_thread, NULL);
	ast_mutex_lock(&client_lock);
	ast_mutex_lock(&start_lock);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (loop_args_container[i]) {
			ast_free(loop_args_container[i]->args);
			ast_free(loop_args_container[i]);
		}
	}

	ast_mutex_destroy(&client_lock);
	ast_cond_destroy(&start_cond);
	ast_mutex_destroy(&start_lock);

}

int respond_to_amqp_message(struct amqp_message *msg)
{
	ast_log(LOG_ERROR, "trying to respond to message...\n");
	ast_mutex_lock(&client_lock);
	int res = 0;
	const char *routing_key = amqp_message_routing_key(msg);
	const char *exchange = amqp_message_exchange(msg);
	for (int i = 0; i < MAX_LOOPS; ++i) {
		if (!strcmp(loop_args_container[i]->args->exchange, exchange)) {
			for (int j = 0; j < 100; ++j) {
				if (!strcmp(loop_args_container[i]->args->handlers[j].routing_key, routing_key)) {
					ast_log(LOG_ERROR, "responding...\n");
					loop_args_container[i]->args->handlers[j].handler((void*) amqp_message_body(msg));
					break;
				}
			}
		}
	}
	ast_mutex_unlock(&client_lock);
	return res;
}
