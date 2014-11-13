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

## dev

Don't forget to tag releases properly.

    git tag v$(head -1 VERSION)
    git push --tags
