require 'active_record'

module ActiveRecord
  class MasterUnavailable < ConnectionNotEstablished; end

  class Base
    class << self
      def with_consistency(clock, &blk)
        if connection.respond_to? :with_consistency
          connection.with_consistency(clock, &blk)
        else
          yield
          nil
        end
      end

      def with_master(&blk)
        if connection.respond_to? :with_master
          connection.with_master(&blk)
        else
          yield
        end
      end

      def with_slave(&blk)
        if connection.respond_to? :with_slave
          connection.with_slave(&blk)
        else
          yield
        end
      end

      def on_commit(&blk)
        connection.on_commit(&blk) if connection.respond_to? :on_commit
      end

      def on_rollback(&blk)
        connection.on_rollback(&blk) if connection.respond_to? :on_rollback
      end

      def master_slave_connection(config)
        config = massage(config)
        load_adapter(config.fetch(:connection_adapter))
        ConnectionAdapters::MasterSlaveAdapter.new(config, logger)
      end

      def mysql_master_slave_connection(config)
        master_slave_connection(config)
      end

    private

      def massage(config)
        config = config.symbolize_keys
        skip = [ :adapter, :connection_adapter, :master, :slaves ]
        defaults = config.
          reject { |k,_| skip.include?(k) }.
          merge(:adapter => config.fetch(:connection_adapter))
        ([config.fetch(:master)] + config.fetch(:slaves, [])).map do |cfg|
          cfg.symbolize_keys!.reverse_merge!(defaults)
        end
        config
      end

      def load_adapter(adapter_name)
        unless respond_to?("#{adapter_name}_connection")
          begin
            require 'rubygems'
            gem "activerecord-#{adapter_name}-adapter"
            require "active_record/connection_adapters/#{adapter_name}_adapter"
          rescue LoadError
            begin
              require "active_record/connection_adapters/#{adapter_name}_adapter"
            rescue LoadError
              raise %Q{Please install the #{adapter_name} adapter:
                       `gem install activerecord-#{adapter_name}-adapter` (#{$!})"}
            end
          end
        end
      end
    end
  end

  module ConnectionAdapters
    class AbstractAdapter
      alias_method :orig_log_info, :log_info
      def log_info(sql, name, ms)
        connection_name =
          [ @config[:name], @config[:host], @config[:port] ].compact.join(":")
        orig_log_info sql, "[#{connection_name}] #{name || 'SQL'}", ms
      end
    end

    class MasterSlaveAdapter < AbstractAdapter
      class Clock
        include Comparable
        attr_reader :file, :position

        def initialize(file, position)
          raise ArgumentError, "file and postion may not be nil" if file.nil? || position.nil?
          @file, @position = file, position.to_i
        end

        def <=>(other)
          @file == other.file ? @position <=> other.position : @file <=> other.file
        end

        def to_s
          [ @file, @position ].join('@')
        end

        def self.zero
          @zero ||= Clock.new('', 0)
        end

        def self.infinity
          @infinity ||= Clock.new('', Float::MAX.to_i)
        end
      end

      checkout :active?

      def initialize(config, logger)
        super(nil, logger)

        @config = config

        @connections = {}
        @connections[:master] = connect_to_master
        @connections[:slaves] = @config.fetch(:slaves).map { |cfg| connect(cfg, :slave) }

        @disable_connection_test = @config[:disable_connection_test] == 'true'

        self.current_connection = slave_connection!
      end

      # MASTER SLAVE ADAPTER INTERFACE ========================================

      def with_master
        with(master_connection) { yield }
      end

      def with_slave
        with(slave_connection!) { yield }
      end

      def with_consistency(clock)
        raise ArgumentError, "consistency cannot be nil" if clock.nil?
        # try random slave, else fall back to master
        slave = slave_connection!
        conn =
          if !open_transaction? && slave_consistent?(slave, clock)
            slave
          else
            master_connection
          end

        with(conn) { yield }

        self.current_clock || clock
      end

      def on_commit(&blk)
        on_commit_callbacks.push blk
      end

      def on_rollback(&blk)
        on_rollback_callbacks.push blk
      end


      # backwards compatibility
      class << self
        def with_master(&blk)
          ActiveRecord::Base.with_master(&blk)
        end
        def with_slave(&blk)
          ActiveRecord::Base.with_slave(&blk)
        end
        def with_consistency(clock, &blk)
          ActiveRecord::Base.with_consistency(clock, &blk)
        end
        def reset!
          Thread.current[:master_slave_clock]      =
          Thread.current[:master_slave_connection] =
          Thread.current[:on_commit_callbacks]     =
          Thread.current[:on_rollback_callbacks]   =
          nil
        end
      end

      # ADAPTER INTERFACE OVERRIDES ===========================================

      def insert(*args)
        on_write { |conn| conn.insert(*args) }
      end

      def update(*args)
        on_write { |conn| conn.update(*args) }
      end

      def delete(*args)
        on_write { |conn| conn.delete(*args) }
      end

      def commit_db_transaction
        on_write { |conn| conn.commit_db_transaction }
        on_commit_callbacks.shift.call(current_clock) until on_commit_callbacks.blank?
      end

      def rollback_db_transaction
        on_commit_callbacks.clear
        with(master_connection) { |conn| conn.rollback_db_transaction }
        on_rollback_callbacks.shift.call until on_rollback_callbacks.blank?
      end

      def active?
        return true if @disable_connection_test
        connections.map { |c| c.active? }.all?
      end

      def reconnect!
        connections.each { |c| c.reconnect! }
      end

      def disconnect!
        connections.each { |c| c.disconnect! }
      end

      def reset!
        connections.each { |c| c.reset! }
      end

      def cache(&block)
        connections.inject(block) do |block, connection|
          lambda { connection.cache(&block) }
        end.call
      end

      def uncached(&block)
        connections.inject(block) do |block, connection|
          lambda { connection.uncached(&block) }
        end.call
      end

      def clear_query_cache
        connections.each { |connection| connection.clear_query_cache }
      end

      # Someone calling execute directly on the connection is likely to be a
      # write, respectively some DDL statement. People really shouldn't do that,
      # but let's delegate this to master, just to be sure.
      def execute(*args)
        on_write { |conn| conn.execute(*args) }
      end

      # ADAPTER INTERFACE DELEGATES ===========================================

      def self.rescued_delegate(*methods, options)
        to, fallback = options.values_at(:to, :fallback)

        file, line = caller.first.split(':', 2)
        line = line.to_i

        methods.each do |method|
          module_eval(<<-EOS, file, line)
            def #{method}(*args, &block)
              begin
                #{to}.__send__(:#{method}, *args, &block)
              rescue => exception
                if connection_error?(exception)
                  reset_master_connection
                  #{fallback ? "#{fallback}.__send__(:#{method}, *args, *block)" : "raise MasterUnavailable"}
                else
                  raise
                end
              end
            end
          EOS
        end
      end
      class << self; private :rescued_delegate; end

      # === must go to master
      rescued_delegate :adapter_name,
                       :supports_migrations?,
                       :supports_primary_key?,
                       :supports_savepoints?,
                       :native_database_types,
                       :raw_connection,
                       :open_transactions,
                       :increment_open_transactions,
                       :decrement_open_transactions,
                       :transaction_joinable=,
                       :create_savepoint,
                       :rollback_to_savepoint,
                       :release_savepoint,
                       :current_savepoint_name,
                       :begin_db_transaction,
                       :outside_transaction?,
                       :add_limit!,
                       :default_sequence_name,
                       :reset_sequence!,
                       :insert_fixture,
                       :empty_insert_statement,
                       :case_sensitive_equality_operator,
                       :limited_update_conditions,
                       :insert_sql,
                       :update_sql,
                       :delete_sql,
                       :sanitize_limit,
                       :to => :master_connection
      # schema statements
      rescued_delegate :native_database_types,
                       :table_exists?,
                       :create_table,
                       :change_table,
                       :rename_table,
                       :drop_table,
                       :add_column,
                       :remove_column,
                       :remove_columns,
                       :change_column,
                       :change_column_default,
                       :rename_column,
                       :add_index,
                       :remove_index,
                       :remove_index!,
                       :rename_index,
                       :index_name,
                       :index_exists?,
                       :structure_dump,
                       :dump_schema_information,
                       :initialize_schema_migrations_table,
                       :assume_migrated_upto_version,
                       :type_to_sql,
                       :add_column_options!,
                       :distinct,
                       :add_order_by_for_association_limiting!,
                       :add_timestamps,
                       :remove_timestamps,
                       :quoted_columns_for_index,
                       :options_include_default?,
                       :to => :master_connection
      # ActiveRecord 3.0
      rescued_delegate :visitor,
                       :to => :master_connection
      # no clear interface contract:
      rescued_delegate :tables,         # commented in SchemaStatements
                       :truncate_table, # monkeypatching database_cleaner gem
                       :primary_key,    # is Base#primary_key meant to be the contract?
                       :to => :master_connection
      # No need to be so picky about these methods
      rescued_delegate :add_limit_offset!, # DatabaseStatements
                       :add_lock!, #DatabaseStatements
                       :columns,
                       :table_alias_for,
                       :to => :master_connection,
                       :fallback => :slave_connection!

      # ok, we might have missed more
      def method_missing(name, *args, &blk)
        master_connection.send(name.to_sym, *args, &blk).tap do
          @logger.try(:warn, %Q{
            You called the unsupported method '#{name}' on #{self.class.name}.
            In order to help us improve master_slave_adapter, please report this
            to: https://github.com/soundcloud/master_slave_adapter/issues

            Thank you.
          })
        end
      rescue => exception
        if connection_error?(exception)
          reset_master_connection
          raise MasterUnavailable
        else
          raise
        end
      end

      # === determine read connection
      delegate :select_all,
               :select_one,
               :select_rows,
               :select_value,
               :select_values,
               :to => :connection_for_read

      def connection_for_read
        open_transaction? ? master_connection : current_connection
      end
      private :connection_for_read

      # === doesn't really matter, but must be handled by underlying adapter
      delegate *(ActiveRecord::ConnectionAdapters::Quoting.instance_methods + [{
               :to => :current_connection }])
      # issue #4: current_database is not supported by all adapters, though
      delegate :current_database, :to => :current_connection

      # UTIL ==================================================================

      def master_connection
        # TODO: add circuit breaker
        @connections[:master] ||= connect_to_master
        @connections[:master] || raise(MasterUnavailable)
      end

      def master_available?
        !@connections[:master].nil?
      end

      def reset_master_connection
        @connections[:master] = nil
      end

      # Returns a random slave connection
      # Note: the method is not referentially transparent, hence the bang
      def slave_connection!
        @connections[:slaves].sample
      end

      def connections
        @connections.values.inject([]) { |m,c| m << c }.flatten.compact
      end

      def current_connection
        connection_stack.first
      end

      def current_clock
        Thread.current[:master_slave_clock]
      end

      def current_clock=(clock)
        Thread.current[:master_slave_clock] = clock
      end

      def master_clock
        conn = master_connection
        # TODO: should be extracted into adapter specific code
        if status = conn.uncached { conn.select_one("SHOW MASTER STATUS") }
          Clock.new(status['File'], status['Position'])
        end
      end

      def slave_clock!
        slave_clock(slave_connection!)
      end

      def slave_clock(conn)
        # TODO: should be extracted into adapter specific code
        if status = conn.uncached { conn.select_one("SHOW SLAVE STATUS") }
          Clock.new(status['Relay_Master_Log_File'], status['Exec_Master_Log_Pos']).tap do |c|
            set_last_seen_slave_clock(conn, c)
          end
        end
      end

      def slave_consistent?(conn, clock)
        get_last_seen_slave_clock(conn).try(:>=, clock) ||
          slave_clock(conn).try(:>=, clock)
      end

    protected

      def on_write
        with(master_connection) do |conn|
          yield(conn).tap do
            unless open_transaction?
              if mc = master_clock
                self.current_clock = mc unless current_clock.try(:>=, mc)
              end
              # keep using master after write
              connection_stack.replace([ conn ])
            end
          end
        end
      end

      def with(conn)
        self.current_connection = conn
        yield(conn).tap { connection_stack.shift if connection_stack.size > 1 }
      end

    private

      def connect(cfg, name)
        adapter_method = "#{cfg.fetch(:adapter)}_connection".to_sym
        ActiveRecord::Base.send(adapter_method, { :name => name }.merge(cfg))
      end

      def open_transaction?
        master_available? ? (master_connection.open_transactions > 0) : false
      end

      def connection_stack
        Thread.current[:master_slave_connection] ||= []
      end

      def current_connection=(conn)
        connection_stack.unshift(conn)
      end

      def on_commit_callbacks
        Thread.current[:on_commit_callbacks] ||= []
      end

      def on_rollback_callbacks
        Thread.current[:on_rollback_callbacks] ||= []
      end

      def get_last_seen_slave_clock(conn)
        conn.instance_variable_get(:@last_seen_slave_clock)
      end

      def set_last_seen_slave_clock(conn, clock)
        last_seen = get_last_seen_slave_clock(conn)
        if last_seen.nil? || last_seen < clock
          conn.instance_variable_set(:@last_seen_slave_clock, clock)
        end
      end

      def connect_to_master
        connect(@config.fetch(:master), :master)
      rescue => exception
        connection_error?(exception) ? nil : raise
      end

      # TODO: should be extracted into adapter specific code
      def connection_error?(exception)
        connection_errors = [
          Mysql::Error::CR_CONNECTION_ERROR,  # query: not connected
          Mysql::Error::CR_CONN_HOST_ERROR,   # Can't connect to MySQL server on '%s' (%d)
          Mysql::Error::CR_SERVER_GONE_ERROR, # MySQL server has gone away
          Mysql::Error::CR_SERVER_LOST,       # Lost connection to MySQL server during query
        ]

        case exception
        when ActiveRecord::MasterUnavailable
          true
        when ActiveRecord::StatementInvalid
          connection_errors.include?(current_connection.raw_connection.errno)
        when Mysql::Error
          connection_errors.include?(exception.errno)
        else
          false
        end
      end
    end
  end
end
