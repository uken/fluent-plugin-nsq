require 'test/unit'

require 'fluent/test'
require 'fluent/plugin/out_nsq'

require 'date'

require 'helper'

$:.push File.expand_path("../lib", __FILE__)
$:.push File.dirname(__FILE__)

class TestNSQOutput < Test::Unit::TestCase
  TCONFIG = %[
    nsqlookupd localhost:4161
    topic logs
  ]
  def setup
    #Nsq.logger = Logger.new(STDOUT)
    Fluent::Test.setup
  end

  def test_configure
    d = create_driver
    assert_not_nil d.instance.topic
  end

  def create_driver(tag='test', conf=TCONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::NSQOutput, tag).configure(conf, true)
  end

  def sample_record
    {'age' => 26, 'request_id' => '42', 'parent_id' => 'parent', 'sub' => {'field'=>{'pos'=>15}}}
  end

  def test_wrong_config
    assert_raise Fluent::ConfigError do
      d = create_driver('test','')
    end
  end

  def test_sample_record_loop
    d = create_driver
    100.times.each do |t|
      d.emit(sample_record)
    end
    d.run
  end
end
