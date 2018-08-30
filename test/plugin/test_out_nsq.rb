require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'securerandom'
require 'json'
require 'timeout'
require_relative '../../lib/fluent/plugin/out_nsq'

class TestNSQOutput < Test::Unit::TestCase

  LOGS_DIR = '/tmp/fluent-plugin-nsq-tests'
  MAX_TOPIC_LENGTH = 64
  MAX_MESSAGE_SIZE = 1024
  MAX_BODY_SIZE = 5 * 1024

  include Fluent::Test::Helpers

  setup do
    Fluent::Test.setup
  end

  def create_config_for_topic(topic_name)
    %[
    nsqd localhost:4151
    topic #{topic_name}
    ]
  end

  def get_random_test_id
    'test_' + SecureRandom.hex[0, 10]
  end

  def create_driver(conf = {}, handle_write_errors = false)
    if !handle_write_errors
      Fluent::Test::Driver::Output.new(Fluent::Plugin::NSQOutput).configure(conf)
    else
      d = Fluent::Test::Driver::Output.new(Fluent::Plugin::NSQOutput) do
        alias old_write write

        def raised_exceptions
          return @raised_exceptions
        end

        def write(chunk)
          @raised_exceptions = []
          old_write chunk
        rescue => e
          @raised_exceptions << e
        end
      end
      d.configure(conf)
    end
  end

  def send_messages(driver, messages, tag = 'test')
    messages_records = messages.map {|message| [event_time, {"message" => message}]}
    es = Fluent::ArrayEventStream.new(messages_records)
    driver.run do
      driver.feed(tag, es)
    end
  end

  def assert_messages_received(test_id, messages)
    wait_for_queue_to_clean test_id
    log_file_loc = "#{LOGS_DIR}/#{test_id}.log"
    assert_equal(true, File.file?(log_file_loc), "log file doesn't exists for test_id: #{log_file_loc}")
    assert_all_messages_in_file(messages, log_file_loc)
  end

  def assert_no_messages_received(topic)
    wait_for_queue_to_clean topic
    log_file_loc = "#{LOGS_DIR}/#{topic}.log"
    assert_equal(false, File.file?(log_file_loc), "log file exists although it should not: #{log_file_loc}")
  end

  def assert_all_messages_in_file(messages, log_file_loc)
    messages_from_file = extract_messages_from_file log_file_loc
    assert_equal(messages_from_file.length, messages.length, "Messages count in log file is different that expected: expected: #{messages.length} actual:#{messages_from_file.length}, file: #{log_file_loc}")
    # TODO change to regular comparison
    messages_xor = messages + messages_from_file - (messages & messages_from_file)
    assert_equal(0, messages_xor.length, "Messages in log file are different than expected ones: expected: #{messages}, actual: (in file: #{log_file_loc}) #{messages_from_file}")
  end

  def extract_messages_from_file(file_location)
    messages = Set.new
    File.open(file_location) do |file|
      file.each do |line|
        parsed_line = JSON.parse(line)
        message = parsed_line["message"]
        assert_not_nil(message, "field 'message' does not exists in line [log file: #{file_location}]")
        assert_not_nil(messages.add?(message), "duplicated message in file #{file_location}")
      end
    end
    messages
  end

  def wait_for_queue_to_clean(topic)
    if topic_exists? topic
      Timeout::timeout(60) do
        all_messages_processed = false
        until all_messages_processed
          sleep 0.01
          topic_stats = get_stats_for_topic topic
          if topic_stats["depth"] != 0
            next
          end
          if topic_stats["message_count"] != 0
            if topic_stats["channels"] && topic_stats["channels"].length == 1
              nsq_to_file_chan = topic_stats["channels"][0]
              if nsq_to_file_chan["depth"] == 0 && topic_stats["message_count"] == nsq_to_file_chan["message_count"]
                all_messages_processed = true
              end
            end
          end
        end
      end
    end
  end

  def get_stats_for_topic(topic)
    stats_response = RestClient.get("http://localhost:4151/stats?topic=#{topic}&format=json")
    parsed_response = JSON.parse(stats_response)
    topics = parsed_response["data"]["topics"]
    if topics && topics.length == 1
      topics[0]
    else
      nil
    end
  end

  def topic_exists?(topic)
    topic_exists = false
    begin
      topic_exists = Timeout::timeout(2) do
        until get_stats_for_topic topic != nil do
          sleep 0.01
        end
        true
      end
    rescue Timeout::Error
      false
    end
    topic_exists
  end

  def assert_request_failed(driver, expected_message)
    assert_equal(1, driver.instance.raised_exceptions.length)
    raised_exception = driver.instance.raised_exceptions.first
    assert_kind_of(RestClient::RequestFailed, raised_exception)
    assert_not_nil(raised_exception.response)
    assert_includes(raised_exception.response.to_s, expected_message)
  end

  test 'send a single message to nsq' do
    test_id = get_random_test_id
    d = create_driver(config = create_config_for_topic(test_id))

    messages = Set['message1']
    send_messages(d, messages)

    assert_messages_received(test_id, messages)
  end

  test 'send messages to nsq' do
    test_id = get_random_test_id
    d = create_driver(config = create_config_for_topic(test_id))

    messages = Set['message1', 'message2', 'message3']
    send_messages(d, messages)

    assert_messages_received(test_id, messages)
  end

  test 'send messages with a topic that exceeds 64 chars' do
    too_long_topic_name = "a" * (MAX_TOPIC_LENGTH + 1)
    d = create_driver(create_config_for_topic(too_long_topic_name), handle_write_errors = true)

    messages = Set['message1', 'message2', 'message3']
    send_messages(d, messages)

    assert_request_failed(d, "INVALID_TOPIC")
    assert_no_messages_received(too_long_topic_name)
  end

  test 'send a message with a length that exceeds MAX_MESSAGE_SIZE' do
    test_id = get_random_test_id
    d = create_driver(config = create_config_for_topic(test_id), handle_write_errors = true)

    too_big_message = "a" * (MAX_MESSAGE_SIZE + 1)
    messages = Set[too_big_message]
    send_messages(d, messages)

    assert_request_failed(d, "MSG_TOO_BIG")
    assert_no_messages_received(test_id)

  end

  test 'send messages with a total sum that exceeds MAX_BODY_SIZE' do
    test_id = get_random_test_id
    d = create_driver(config = create_config_for_topic(test_id), handle_write_errors = true)

    messages = Array.new(MAX_BODY_SIZE + 1, "a")
    send_messages(d, messages)

    assert_request_failed(d, "BODY_TOO_BIG")
    assert_no_messages_received(test_id)
  end

end