safe_require('jdbc/postgres')

if defined?(Jdbc::Postgres)
  Jdbc::Postgres.load_driver

  module Purview
    module RawConnections
      module JDBC
        class Postgres < Base
          private

          def url
            "jdbc:postgresql://#{host}#{port && ":#{port}"}/#{database}"
          end

          def username
            super || ENV['PGUSER'] || Etc.getlogin
          end
        end
      end
    end
  end

  Purview::RawConnections::PostgreSQL = Purview::RawConnections::JDBC::Postgres
end
