$: << File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'lib'))

require 'rspec'
require 'master_slave_adapter'
require 'integration/helpers/mysql_helper'

describe "Test" do
  include MysqlHelper

  let(:configuration) do
    {
      :adapter => 'master_slave',
      :connection_adapter => 'mysql',
      :username => 'root',
      :database => 'master_slave_adapter',
      :master => {
        :host => '127.0.0.1',
        :port => port(:master),
      },
      :slaves => [{
        :host => '127.0.0.1',
        :port => port(:slave),
      }],
    }
  end

  def connection
    ActiveRecord::Base.connection
  end

  def should_read_from(host)
    server = server_id(host)
    query  = "SELECT @@Server_id as Value"

    connection.select_all(query).first["Value"].to_s.should == server
    connection.select_one(query)["Value"].to_s.should == server
    connection.select_rows(query).first.first.to_s.should == server
    connection.select_value(query).to_s.should == server
    connection.select_values(query).first.to_s.should == server
  end

  before(:all) do
    setup
    start_master
    start_slave
    configure
    start_replication
  end

  after(:all) do
    stop_master
    stop_slave
  end

  before do
    ActiveRecord::Base.establish_connection(configuration)
  end

  context "given slave lags behind" do
    before do
      stop_replication
      puts "Stopped replication. Slave clock is at #{send(:status, :slave)[4..5].inspect}"
      move_master_clock
    end

    context "and slave catches up" do
      before do
        start_replication
        move_master_clock # needed to read slave clock deterministically
        wait_for_replication_sync
      end

      100.times do
        it "reads from slave" do
          ActiveRecord::Base.with_consistency(connection.master_clock) do
            should_read_from :slave
          end
        end
      end
    end
  end
end
