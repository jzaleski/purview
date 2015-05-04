module Purview
  module Pullers
    class PostgreSQL < BaseSQL
      private

      def connection_type
        Purview::Connections::PostgreSQL
      end

      def null_value
        'NULL'
      end
    end
  end
end
