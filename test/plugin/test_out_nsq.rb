require 'test/unit'

require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_nsq'

require 'date'

require 'helper'

$:.push File.expand_path("../lib", __FILE__)
$:.push File.dirname(__FILE__)

class TestNSQOutput < Test::Unit::TestCase
  TCONFIG = %[
    nsqlookupd localhost:4161
    topic logs_out
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
    Fluent::Test::Driver::Output.new(Fluent::Plugin::NSQOutput).configure(conf)
  end

  def sample_record
    {'age' => 26, 'request_id' => '42', 'parent_id' => 'parent', 'sub' => {'field'=>{'pos'=>15}}}
  end

  def test_wrong_config
    assert_raise Fluent::ConfigError do
      create_driver('')
    end
  end

  def test_sample_record_loop
    d = create_driver
    d.run(default_tag: 'test') do
      100.times.each do |t|
        d.feed(sample_record)
      end
    end
  end
end
