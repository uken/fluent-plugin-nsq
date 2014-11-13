# coding: utf-8

module Fluent
  class NSQInput < Input
    Plugin.register_input('nsq', self)

    config_param :topic, :string, default: nil
    config_param :channel, :string, default: 'fluent_nsq_input'
    config_param :nsqlookupd, :string, default: nil
    config_param :tag, :string, default: '_key'
    config_param :time_key, :string, default: nil
    config_param :tag_source, default: :key do |val|
      case val.downcase
      when 'key'
        :key
      when 'topic'
        :topic
      when 'static'
        :static
      else
        fail ConfigError, 'tag_source should be either "key", "static" or "topic"'
      end
    end

    def initialize
      super
      require 'cool.io'
      require 'nsq'
    end

    def configure(conf)
      super

      fail ConfigError, 'Missing nsqlookupd' unless @nsqlookupd
      fail ConfigError, 'Missing topic' unless @topic
      fail ConfigError, 'Missing channel' unless @channel
    end

    def start
      super
      lookupds = @nsqlookupd.split(',')
      @consumer = Nsq::Consumer.new(
        nsqlookupd: lookupds,
        topic: @topic,
        channel: @channel
      )
      @running = true
      @thread = Thread.new(&method(:consume))
    end

    def shutdown
      super
      @running = false
      @consumer.terminate
    end

    private
    def consume
      while @running
        consume_one
      end
    end

    def consume_one
      msg = @consumer.pop
      record = JSON.parse(msg.body)
      record_tag = tag_for_record(record)
      record_time = time_for_record(record, msg)
      Engine.emit(record_tag, record_time, record)
      msg.finish
    rescue => e
      log.warn("nsq: #{e}")
      msg.requeue if msg
    end

    def tag_for_record(record)
      case @tag_source
      when :static
        @tag
      when :key
        record[@tag]
      when :topic
        @topic
      end
    end

    def time_for_record(record, msg)
      if @time_key
        record[@time_key]
      else
        msg.timestamp
      end
    end
  end
end