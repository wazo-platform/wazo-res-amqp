/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright 2019 The Wazo Authors  (see the AUTHORS file)
 *
 * Nicolaos Ballas <nballas@wazo.io>
 *
 */

#include "asterisk.h"

#include "asterisk/module.h"
#include "asterisk/amqp.h"

static int amqp_basic_callback(void *args)
{
	ast_log(LOG_ERROR, "I got %s \n", (char*) args);

	return 0;
}

static int amqp_another_callback(void *args)
{
	const char *string = args;
	char *copy = ast_calloc(strlen(string) + 1, sizeof(char));
	strcpy(copy, string);
	for(size_t i = 0; i < strlen(copy); ++i){
   		if((copy[i] > 96) && (copy[i] < 123)){
   			copy[i] ^= 0x20;
   		}
	}
	ast_log(LOG_ERROR, "uppercase:  %s\n", copy);
	ast_free(copy);
	return 0;
}

static int load_module(void)
{
	if (amqp_start_client("bunny", "xivo")) {
		return AST_MODULE_LOAD_DECLINE;
	}
	const char *connection_name = "bunny";
	ast_log(LOG_ERROR, "Loading example module...\n");
	amqp_register_callback(connection_name, "xivo", "key", amqp_basic_callback);
	amqp_register_callback(connection_name, "xivo", "another_key", amqp_another_callback);
	ast_log(LOG_ERROR, "Loaded example module!\n");


	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{

	return 0;
}


AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_GLOBAL_SYMBOLS | AST_MODFLAG_LOAD_ORDER, "Example AMQP callback registration",
	.support_level = AST_MODULE_SUPPORT_CORE,
	.load = load_module,
	.unload = unload_module,
	.requires = "res_amqp",
	.load_pri = AST_MODPRI_APP_DEPEND,
	);