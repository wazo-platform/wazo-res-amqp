/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2019 The Wazo Authors  (see the AUTHORS file)
 *
 * Nicolaos Ballas <nballas@wazo.io>
 *
 */
#ifndef ASTERISK_AMQP_MESSAGE_H
#define ASTERISK_AMQP_MESSAGE_H

#include <amqp.h>
#include <amqp_framing.h>

struct amqp_message;

struct amqp_message *amqp_message_create_blank(void);

struct amqp_message *amqp_message_create_from_envelope(const amqp_envelope_t *envelope);

void amqp_message_destroy(struct amqp_message *msg);

const char *amqp_message_body(struct amqp_message *msg);

const char *amqp_message_routing_key(struct amqp_message *msg);

const char *amqp_message_exchange(struct amqp_message *msg);

#endif //ASTERISK_AMQP_MESSAGE_H
