safe_require('mysql2')

if defined?(Mysql2)
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
            filter_blank_values(
              :database => database.to_s,
              :host => host.to_s,
              :password => password.to_s,
              :port => port,
              :username => username.to_s
            )
          )
        end

        def username
          super || ENV['USER'] || Etc.getlogin
        end
      end
    end
  end

  Purview::RawConnections::MySQL = Purview::RawConnections::Mysql2
end
