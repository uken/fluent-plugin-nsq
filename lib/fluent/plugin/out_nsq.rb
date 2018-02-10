# coding: utf-8

module Fluent
  class NSQOutput < Output
    Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqd, :string, default: nil
    config_param :use_tls, :bool, default: false
    config_param :tls_key, :string, default: nil
    config_param :tls_cert, :string, default: nil

    def initialize
      super
      require 'nsq'
      require 'yajl'

      log.info("nsq: initialize called!")
    end

    def configure(conf)
      super

      log.info("nsq: configure called! @nsqd=#{@nsqd}, @topic=#{@topic}, @use_tls=#{@use_tls}, @tls_key=#{@tls_key}, @tls_cert=#{@tls_cert}")

      fail ConfigError, 'Missing nsqd' unless @nsqd

      if @use_tls
        fail ConfigError, 'Missing TLS key' unless @tls_key
        fail ConfigError, 'Missing TLS key' unless @tls_cert
      end
    end

    def start
      super

      log.info("nsq: start called!")

      nsq_producer_opts = {
        nsqd: @nsqd,
      }

      if @use_tls
        nsq_producer_opts.update({
          tls_v1: true,
          tls_options: {
              key: @tls_key,
              certificate: @tls_cert,
              verify_mode: OpenSSL::SSL::VERIFY_NONE
          }
        })
      end

      @producer = Nsq::Producer.new(nsq_producer_opts)
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

      message_batch_by_topic = Hash.new { |hash, key| hash[key] = [] }

      chunk.msgpack_each do |tag, time, record|
        next unless record.is_a? Hash
        record.update(
          :_key => tag,
          :_ts => time,
          :'@timestamp' => Time.at(time).to_datetime.to_s  # kibana/elasticsearch friendly
        )
        serialized_record = Yajl.dump(record)

        if record.key?("NSQTopic")
          message_batch_by_topic[record["NSQTopic"]] << serialized_record
        elsif not @topic.nil?
          message_batch_by_topic[@topic] << serialized_record
        else
          log.warn("nsq: can't write to nsq without default topic!")
        end
      end

      message_batch_by_topic.each do |topic, messages|
        log.debug("nsq: posting #{messages.length} messages to topic #{topic}")
        @producer.write_to_topic(topic, *messages)
      end
    end
  end
end
