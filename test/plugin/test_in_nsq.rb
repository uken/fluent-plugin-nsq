require 'test/unit'

require 'fluent/test'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_nsq'

require 'date'

require 'helper'

$:.push File.expand_path("../lib", __FILE__)
$:.push File.dirname(__FILE__)

class TestNSQInput < Test::Unit::TestCase
  TCONFIG = %[
    nsqlookupd localhost:4161
    topic logs_in
    time_key _ts
  ]
  def setup
    #Nsq.logger = Logger.new(STDOUT)
    Fluent::Test.setup
  end

  def test_configure
    d = create_driver
    assert_not_nil d.instance.topic
  end

  def create_driver(conf=TCONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::NSQInput).configure(conf)
  end

  def create_producer
    Nsq::Producer.new(
      nsqlookupd: ['127.0.0.1:4161'],
      topic: 'logs_in'
    )
  end

  def sample_record
    {_ts: Time.now, _key: 'somekey', age:26, request_id: '42', parent_id: 'parent', sub: {field: {pos: 15}}}
  end

  def test_wrong_config
    assert_raise Fluent::ConfigError do
      create_driver('')
    end
  end

  def test_sample_record_loop
    d = create_driver
    d.run do
      prod = create_producer
      sleep(1)
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      sleep(1)
      prod.terminate
    end
    puts("emitz")
    puts(d.events)
  end
end
