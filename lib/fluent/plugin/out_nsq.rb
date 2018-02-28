# coding: utf-8

module Fluent::Plugin
  class NSQOutput < Output
    Fluent::Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqd, :string, default: nil
    config_param :use_tls, :bool, default: false
    config_param :tls_key, :string, default: nil
    config_param :tls_cert, :string, default: nil

    helpers :compat_parameters, :inject

    def initialize
      super
      require 'nsq'
      require 'yajl'

      log.info("nsq: initialize called!")
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject)
      super

      log.info("nsq: configure called! @nsqd=#{@nsqd}, @topic=#{@topic}, @use_tls=#{@use_tls}, @tls_key=#{@tls_key}, @tls_cert=#{@tls_cert}")

      fail ConfigError, 'Missing nsqd' unless @nsqd
      fail ConfigError, 'Missing topic' unless @topic

      if @use_tls
        fail ConfigError, 'Missing TLS key' unless @tls_key
        fail ConfigError, 'Missing TLS key' unless @tls_cert
      end
    end

    def start
      super

      log.info("nsq: start called!")

      nsq_producer_opts = {
        nsqd: @nsqd.split(","),
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
      record = inject_values_to_record(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def write(chunk)
      return if chunk.empty?

      message_batch = []

      chunk.msgpack_each do |tag, time, record|
        next unless record.is_a? Hash
        record.update(
          :_key => tag,
          :_ts => time,
          :'@timestamp' => Time.at(time).to_datetime.to_s  # kibana/elasticsearch friendly
        )
        serialized_record = Yajl.dump(record)

        message_batch << serialized_record
      end

      topic = extract_placeholders(@topic, chunk.metadata)

      log.debug("nsq: posting #{message_batch.length} messages to topic #{topic}")
      @producer.write_to_topic(topic, *message_batch)
    end
  end
end
