module Purview
  module Pullers
    class PostgreSQL < BaseSQL
      private

      def connection_type
        Purview::Connections::PostgreSQL
      end

      def dialect_type
        Purview::Dialects::PostgreSQL
      end
    end
  end
end
