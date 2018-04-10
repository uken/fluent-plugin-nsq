# coding: utf-8

module Fluent
  class NSQOutput < BufferedOutput
    Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqlookupd, :string, default: nil

    def initialize
      super
      require 'nsq'
      require 'yajl'
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
      @producer.terminate
      super
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
        begin
          @producer.write(Yajl.dump(tagged_record))
        rescue => e
          log.warn("nsq: #{e}")
        end
      end
    end
  end
end
