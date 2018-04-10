require 'test/unit'

require 'fluent/test'
require 'fluent/plugin/in_nsq'
require 'nsq-cluster'

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

  def teardown
    @cluster.destroy if @cluster
  end

  def test_configure
    d = create_driver
    assert_not_nil d.instance.topic
  end

  def create_driver(conf=TCONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::NSQInput).configure(conf)
  end

  def create_producer
    @cluster = NsqCluster.new(nsqd_count: 3, nsqlookupd_count: 2)
    nsqlookupd = @cluster.nsqlookupd.first
    Nsq::Producer.new(
      nsqlookupd: "#{nsqlookupd.host}:#{nsqlookupd.http_port}",
      topic: 'logs_in',
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
      connected = false
      while(!connected) do
        connected = prod.connected?
        break if connected
        sleep(1)
      end
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      prod.write(sample_record.to_json)
      sleep(1)
      prod.terminate
    end
    puts("emitz")
    puts(d.emits)
  end
end
