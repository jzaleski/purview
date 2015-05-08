if defined?(JRUBY_VERSION) && (require 'jdbc/postgres' rescue nil)
  Jdbc::Postgres.load_driver

  module Purview
    module RawConnections
      module JDBC
        class Postgres < Base
          private

          def engine
            'postgresql'
          end
        end
      end
    end
  end

  Purview::RawConnections::PostgreSQL = Purview::RawConnections::JDBC::Postgres
end
