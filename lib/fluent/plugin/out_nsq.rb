# coding: utf-8

require 'fluent/plugin/output'
require 'nsq'
require 'yajl'

module Fluent::Plugin
  class NSQOutput < Output
    Fluent::Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqlookupd, :array, default: nil

    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
    end

    def configure(conf)
      super

      fail Fluent::ConfigError, 'Missing nsqlookupd' unless @nsqlookupd
      fail Fluent::ConfigError, 'Missing topic' unless @topic
    end

    def start
      super
      @producer = Nsq::Producer.new(
        nsqlookupd: @nsqlookupd,
        topic: @topic
      )
    end

    def shutdown
      @producer.terminate
      super
    end

    def write(chunk)
      return if chunk.empty?

      tag = chunk.metadata.tag
      chunk.each do |time, record|
        tagged_record = record.merge(
          :_key => tag,
          :_ts => time,
          :'@timestamp' => Time.at(time).to_datetime.to_s  # kibana/elasticsearch friendly
        )
        begin
          @producer.write(Yajl.dump(tagged_record))
        rescue => e
          log.warn("nsq: #{e}")
        end
      end
    end
  end
end
