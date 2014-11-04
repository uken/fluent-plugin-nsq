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

      raise ConfigError, "Missing nsqlookupd" unless @nsqlookupd
      raise ConfigError, "Missing topic" unless @topic
    end

    def start
      lookupds = @nsqlookupd.split(',')
      @producer = Nsq::Producer.new(
        nslookupd: lookupds,
        topic: @topic
      )
    end

    def shutdown
      @producer.terminate
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      return if chunk.empty?

      chunk.msgpack_each do |tag, time, record|
        next unless record.is_a? Hash
        @producer.write(record.to_json)
      end
    end
  end
end
