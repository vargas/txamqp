## Python library for communicating with AMQP peers and brokers using Twisted

[![Python Test](https://github.com/DomAmato/txamqp/workflows/Python%20Test/badge.svg)](https://github.com/DomAmato/txamqp/actions)
[![Coverage Status](https://coveralls.io/repos/github/DomAmato/txamqp/badge.svg?branch=master)](https://coveralls.io/github/DomAmato/txamqp?branch=master)

This is a quick post on getting started using the txAMQP library with RabbitMQ,
based on Dan Reverri's article "Getting started with RabbitMQ, Python, Twisted,
and txAMQP on Ubuntu 9.04" [http://www.apparatusproject.org/blog/?p=38]

### Install RabbitMQ
	
If you're using Debian (or a derivative, like Ubuntu) and RabbitMQ is not
included in your distribution, you can add RabbitMQ's APT repository:

    http://www.rabbitmq.com/debian.html#apt

### Running RabbitMQ

If you installed RabbitMQ using the Debian package, it will be automatically
started. If not, you'll find an init.d file in /etc/init.d/rabbitmq-server

### Install Twisted

As of 2009-07-09, txAMQP is being developed using Twisted 8.2.0, it may
work with older versions, but it's not guaranteed. In case you are using
Ubuntu, and the version available is not recent enough, you can install
Twisted through its PPA:

    https://launchpad.net/~twisted-dev/+archive/ppa

### Install txAMQP

The instructions below install txAQMP from trunk. There are Ubuntu packages
available if you prefer, refer to the following page for more details:

    https://launchpad.net/~txamqpteam/+archive/ppa

a. Install Bazaar

    $ sudo apt-get install bzr

b. Fetch txAQMP

    $ cd ~
    $ bzr branch lp:txamqp txamqp

If you get the error "bzr: ERROR: Unknown repository format: 'Bazaar
RepositoryFormatKnitPack6 (bzr 1.9)", you'll need to upgrade Bazaar. You may
do so following the instructions at:

    http://bazaar-vcs.org/Download

c. Install txAMQP

    $ cd txamqp
    $ sudo python setup.py install
