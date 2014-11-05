# coding: utf-8
require 'nsq'

module Fluent
  class NSQOutput < BufferedOutput
    Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqlookupd, :string, default: nil

    def initialize
      super
    end

    def configure(conf)
      super

      fail ConfigError, 'Missing nsqlookupd' unless @nsqlookupd
      fail ConfigError, 'Missing topic' unless @topic
    end

    def start
      super
      lookupds = @nsqlookupd.split(',')
      @producer = Nsq::Producer.new(
        nsqlookupd: lookupds,
        topic: @topic
      )
    end

    def shutdown
      super
      @producer.terminate
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      return if chunk.empty?

      chunk.msgpack_each do |tag, time, record|
        next unless record.is_a? Hash
        # TODO get rid of this extra copy
        tagged_record = record.merge(
          :_key => tag,
          :_ts => time,
          :'@timestamp' => Time.at(time).to_datetime.to_s  # kibana/elasticsearch friendly
        )
        @producer.write(tagged_record.to_json)
      end
    end
  end
end
