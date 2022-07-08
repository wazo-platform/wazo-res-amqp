/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2019 The Wazo Authors	(see the AUTHORS file)
 *
 * Nicolaos Ballas <nballas@wazo.io>
 *
 */

#include "amqp_message.h"
#include "asterisk.h"
#include "asterisk/module.h"
#include "asterisk/amqp.h"
#include "amqp/internal.h"

struct amqp_message {
	char *body;
	char *routing_key;
	char *exchange;
};

static char *stringify_bytes(amqp_bytes_t bytes)
{
	/* We will need up to 4 chars per byte, plus the terminating 0 */
	char *res = ast_malloc(bytes.len * 4 + 1);
	if (!res) {
		ast_log(LOG_ERROR, "memory allocation error\n");
		return NULL;
	}
	uint8_t *data = bytes.bytes;
	char *p = res;
	size_t i;

	for (i = 0; i < bytes.len; i++) {
		if (data[i] >= 32 && data[i] != 127) {
			*p++ = data[i];
		} else {
			*p++ = '\\';
			*p++ = '0' + (data[i] >> 6);
			*p++ = '0' + (data[i] >> 3 & 0x7);
			*p++ = '0' + (data[i] & 0x7);
			}
	}

	*p = 0;
	return res;
}

struct amqp_message *amqp_message_create_blank(void)
{
	return ast_calloc(1, sizeof(struct amqp_message));

}

struct amqp_message *amqp_message_create_from_envelope(const amqp_envelope_t *envelope)
{
	if (!envelope) {
		return NULL;
	}
	struct amqp_message *msg = amqp_message_create_blank();
	if (!msg) {
		goto cleanup;
	}
	char *exchange = stringify_bytes(envelope->exchange);
	char *key = stringify_bytes(envelope->routing_key);
	char *message = stringify_bytes(envelope->message.body);

	msg->exchange = exchange;
	msg->routing_key = key;
	msg->body = message;

	return msg;
cleanup:
	amqp_message_destroy(msg);
	return NULL;
}

void amqp_message_destroy(struct amqp_message *msg)
{
	if (!msg) {
		return;
	}
	ast_free(msg->body);
	ast_free(msg->routing_key);
	ast_free(msg->exchange);
	ast_free(msg);
}

const char *amqp_message_body(struct amqp_message *msg)
{
	return msg->body;
}

const char *amqp_message_routing_key(struct amqp_message *msg)
{
	return msg->routing_key;
}

const char *amqp_message_exchange(struct amqp_message *msg)
{
	return msg->exchange;
}
