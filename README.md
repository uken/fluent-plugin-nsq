# NSQ plugin for Fluentd

Input and Output plugins for [nsq](http://nsq.io).

NSQ daemons are discovered through lookupd.

## Installation

    gem install fluent-plugin-nsq

## Usage

### Input

    <source>
      type nsq
      topic webservers
      nsqlookupd 127.0.0.1:4161
      tag_source topic
    </source>

### Output

    <match **>
      type nsq
      buffer_type file
      buffer_path /var/log/fluent/msgbus
      nsqlookupd 127.0.0.1:4161
      topic logs
    </match>

## Local Setup

### Configure RVM:
Install RVM

     $ \curl -sSL https://get.rvm.io | bash -s stable
     $ source /Users/<user>/.rvm/scripts/rvm

Download and use Ruby 2.5.1

     $ rvm install 2.5.1
     $ rvm docs generate-ri  
     $ rvm use 2.5.1

Create a gemset for the project

     $ cd <gitroot>/fluent-plugin-nsq
     $ rvm gemset create fluent-plugin-nsq
     $ rvm --rvmrc ruby-2.5.1@fluent-plugin-nsq
     $ cd ..; cd-
     $ rvm rvmrc to ruby-version

### Install dependencies
    $ bundle install

## Running the tests
     $ cd <gitroot>/fluent-plugin-nsq/docker
     $ docker-compose up
     $ ruby <gitroot>/fluent-plugin-nsq/test/plugin/test_out_nsq.rb