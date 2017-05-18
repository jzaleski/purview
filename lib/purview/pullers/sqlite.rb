module Purview
  module Pullers
    class SQLite < BaseSQL
      private

      def connection_type
        Purview::Connections::SQLite
      end

      def dialect_type
        Purview::Dialects::SQLite
      end
    end
  end
end
