if jruby? && safe_require('jdbc/postgres')
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
