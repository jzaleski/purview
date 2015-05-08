if !defined?(JRUBY_VERSION) && (require 'mysql2' rescue nil)
  module Purview
    module RawConnections
      class Mysql2 < Base
        private

        def execute_sql(sql, opts={})
          raw_connection.query(sql, opts.merge(:cast => false))
        end

        def extract_rows(result)
          result && result.to_a
        end

        def extract_rows_affected(result)
          raw_connection.affected_rows
        end

        def new_connection
          ::Mysql2::Client.new(
            filter_nil_values(
              :database => database,
              :host => host,
              :password => password,
              :port => port,
              :username => username
            )
          )
        end
      end
    end
  end

  Purview::RawConnections::MySQL = Purview::RawConnections::Mysql2
end
