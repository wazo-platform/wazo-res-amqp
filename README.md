To install

    apt-get install librabbitmq-dev
    make
    make install
    make samples

Configure the file in /etc/asterisk/amqp.conf

Please restart asterisk before loading res_amqp.so for the documentation.

To load module

    CLI> module load res_amqp.so

There is a amqp command on the CLI to get the status.

# Current State

* Working, but not consistently
	* Appears to be a race-condition or a threading issue.
	* Client starts, but sometimes freezes and does not seem to respond to events.
	* Not yet sure if only CLI is unresponsive or, AMQP client as a whole, or Asterisk as a whole.
	* Sometimes, a call to `amqp_start_client` sometimes fails. This is possibly linked to
	`amqp_queue_declare` which blocks.
	* Calling `amqp_start_client` with various parameters or even at various locations in the code seems to
	alter its behavior.