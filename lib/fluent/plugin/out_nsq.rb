# coding: utf-8

module Fluent::Plugin

  class NSQOutput < Output
    Fluent::Plugin.register_output('nsq', self)

    config_param :topic, :string, default: nil
    config_param :nsqd, :string, default: nil
    config_param :use_tls, :bool, default: false
    config_param :tls_options, :hash, default: nil, symbolize_keys: true

    helpers :compat_parameters, :inject

    def initialize
      super
      require 'yajl'
      require 'rest-client'

      log.info("nsq: initialize called!")
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, :inject)
      super

      log.info("nsq: configure called! @nsqd=#{@nsqd}, @topic=#{@topic}, @use_tls=#{@use_tls}, @tls_options=#{@tls_options}")

      fail ConfigError, 'Missing nsqd' unless @nsqd
      fail ConfigError, 'Missing topic' unless @topic

      if @use_tls
        fail ConfigError, 'Missing TLS options' unless @tls_options
      end
    end

    def start
      super
      log.info("nsq: start called!")
    end

    def shutdown
      super
      log.info("nsq: shutdown called!")
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def formatted_to_msgpack_binary
      true
    end

    def write(chunk)

      log.info("nsq: write began!")

      return if chunk.empty?

      message_batch = []

      chunk.msgpack_each do |tag, time, record|
        next unless record.is_a? Hash
        record.update(
            :_key => tag,
            :_ts => time,
            :'@timestamp' => Time.at(time).to_datetime.to_s # kibana/elasticsearch friendly
        )
        serialized_record = Yajl.dump(record)

        message_batch << serialized_record
      end

      topic = extract_placeholders(@topic, chunk.metadata)

      log.debug("nsq: posting #{message_batch.length} messages to topic #{topic}")

      write_to_topic_http topic, message_batch
    end

    def write_to_topic_http(topic, messages)
      messages = messages.map(&:to_s)
      if messages.length > 1
        payload = messages.join("\n")
        endpoint = "mpub"
      else
        payload = messages.first
        endpoint = "pub"
      end

      url = "http://#{@nsqd}/#{endpoint}?topic=#{topic}"

      log.debug("url: #{url}")

      RestClient.post(url, payload, headers = {})

    rescue RestClient::RequestFailed => e
      e.message = e.response.to_s
      raise e
    end
  end
end
