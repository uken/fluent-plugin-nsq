# coding: utf-8

module Fluent
  class NSQOutput < BufferedOutput
    Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqd, :string, default: nil

    def initialize
      super
      require 'nsq'
      require 'yajl'

      log.info("nsq: initialize called!")
    end

    def configure(conf)
      super

      log.info("nsq: configure called! @nsqd=#{@nsqd}, @topic=#{@topic}")

      fail ConfigError, 'Missing nsqd' unless @nsqd
    end

    def start
      super

      log.info("nsq: start called!")

      @producer = Nsq::Producer.new(
        nsqd: @nsqd,
        topic: @topic
      )
    end

    def shutdown
      super

      log.info("nsq: shutdown called!")

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
        begin
          if record.key?("NSQTopic")
            @producer.write_to_topic(record["NSQTopic"], Yajl.dump(tagged_record))
          else
            if @topic.nil?
              log.warn("can't write to nsq without default topic!")
            else
              @producer.write(Yajl.dump(tagged_record))
            end
          end
        rescue => e
          log.warn("nsq: #{e}")
        end
      end
    end
  end
end
