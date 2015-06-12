safe_require('tiny_tds')

if defined?(TinyTds)
  module Purview
    module RawConnections
      class TinyTds < Base
        private

        def execute_sql(sql, opts={})
          raw_connection.execute(sql)
        end

        def extract_rows(result)
          result && result.to_a
        end

        def extract_rows_affected(result)
          result.affected_rows
        end

        def new_connection
          ::TinyTds::Client.new(
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
          super || ENV['SQLCMDUSER'] || Etc.getlogin
        end
      end
    end
  end

  Purview::RawConnections::MSSQL = Purview::RawConnections::TinyTds
end
