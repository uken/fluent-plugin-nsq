# NSQ output plugin for Fluentd

Buffers and forwards log entries to [nsq](http://nsq.io) for realtime processing.

NSQ daemons are discovered through lookupd.

## Installation

    gem install fluent-plugin-nsq

## Usage

    <match **>
      type nsq
      buffer_type file
      buffer_path /var/log/fluent/msgbus
      nsqlookupd 127.0.0.1:4161
      topic logs
    </match>

## dev

Don't forget to tag releases properly.

    git tag v$(cat VERSION)
    git push --tags
