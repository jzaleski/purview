safe_require('pg')

if defined?(PG)
  module Purview
    module RawConnections
      class PG < Base
        private

        def execute_sql(sql, opts={})
          raw_connection.exec(sql)
        end

        def extract_rows(result)
          result && result.to_a
        end

        def extract_rows_affected(result)
          result && result.cmd_tuples
        end

        def new_connection
          ::PG.connect(
            filter_nil_values(
              :dbname => database,
              :host => host,
              :password => password,
              :port => port,
              :user => username
            )
          )
        end

        def username
          super || ENV['PGUSER'] || Etc.getlogin
        end
      end
    end
  end

  Purview::RawConnections::PostgreSQL = Purview::RawConnections::PG
end
